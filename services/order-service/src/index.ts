import './lib/load-env.js';
import { loadConfig } from './config.js';
import { runMigrations } from './db/migrate.js';
import { PgOrderAnalytics } from './db/analytics-repo.js';
import { createPool } from './db/pool.js';
import { PgUnitOfWork } from './db/unit-of-work.js';
import { buildApp } from './app.js';
import { createKafka, createProducer, ensureTopics, type ManagedProducer } from './lib/kafka.js';
import { createLogger } from './lib/logger.js';
import type { ReadinessCheck } from './routes/health.js';
import { OrderService } from './services/order-service.js';
import { OrderEventConsumer } from './workers/kafka-consumer.js';
import { createDispatcher } from './workers/message-handler.js';
import { OutboxRelay } from './workers/outbox-relay.js';

const SHUTDOWN_TIMEOUT_MS = 15_000;

/**
 * Composition root: the only place that wires concrete implementations together.
 * Everything below this file receives its dependencies as arguments.
 */
async function main(): Promise<void> {
  const config = loadConfig();
  const logger = createLogger(config);

  const pool = createPool(config, logger);
  if (config.runMigrationsOnStart) {
    await runMigrations(pool, logger);
  }

  const uow = new PgUnitOfWork(pool);
  const service = new OrderService({
    uow,
    logger,
    defaultCurrency: config.defaultCurrency,
  });

  const readinessChecks: ReadinessCheck[] = [
    {
      name: 'postgres',
      check: async () => {
        await pool.query('SELECT 1');
        return true;
      },
    },
  ];

  let app: Awaited<ReturnType<typeof buildApp>> | undefined;
  let managedProducer: ManagedProducer | undefined;
  let relay: OutboxRelay | undefined;
  let consumer: OrderEventConsumer | undefined;
  let shuttingDown = false;

  const shutdown = async (reason: string, exitCode: number): Promise<void> => {
    if (shuttingDown) return;
    shuttingDown = true;
    logger.info({ reason }, 'shutting down');

    // Hard stop if graceful shutdown hangs (e.g. a stuck broker connection).
    setTimeout(() => process.exit(1), SHUTDOWN_TIMEOUT_MS).unref();

    try {
      // Order matters: stop accepting new work first, then drain, then close shared resources.
      await app?.close(); // no new HTTP requests
      await consumer?.stop(); // no new Kafka messages
      await relay?.stop(); // finish the in-flight outbox batch
      await managedProducer?.producer.disconnect();
      await pool.end();
    } catch (err) {
      logger.error({ err }, 'error during shutdown');
      exitCode = 1;
    }
    process.exit(exitCode);
  };

  if (config.enableWorkers) {
    const kafka = createKafka(config, logger);
    if (config.kafkaEnsureTopics) {
      await ensureTopics(kafka, config, logger);
    }

    managedProducer = createProducer(kafka);
    await managedProducer.producer.connect();

    relay = new OutboxRelay({
      uow,
      producer: managedProducer.producer,
      logger,
      pollIntervalMs: config.outboxPollIntervalMs,
      batchSize: config.outboxBatchSize,
      retentionHours: config.outboxRetentionHours,
      idempotencyTtlHours: config.idempotencyTtlHours,
    });
    relay.start();

    consumer = new OrderEventConsumer({
      kafka,
      producer: managedProducer.producer,
      dispatch: createDispatcher(service),
      logger,
      serviceName: config.serviceName,
      groupId: config.kafkaGroupId,
      fromBeginning: config.kafkaFromBeginning,
      retryDelayMs: config.retryDelayMs,
      maxRetryAttempts: config.maxRetryAttempts,
      onFatal: (err) => {
        logger.fatal({ err }, 'kafka consumer crashed permanently');
        void shutdown('consumer_crash', 1);
      },
    });
    await consumer.start();

    const producerRef = managedProducer;
    const relayRef = relay;
    const consumerRef = consumer;
    readinessChecks.push(
      { name: 'kafka-producer', check: async () => producerRef.isConnected() },
      { name: 'kafka-consumer', check: async () => consumerRef.isRunning() },
      { name: 'outbox-relay', check: async () => relayRef.isHealthy() },
    );
  } else {
    logger.warn('workers disabled: running HTTP API only');
  }

  app = await buildApp({ logger, service, analytics: new PgOrderAnalytics(pool), readinessChecks });
  await app.listen({ host: config.host, port: config.port });

  process.on('SIGTERM', () => void shutdown('SIGTERM', 0));
  process.on('SIGINT', () => void shutdown('SIGINT', 0));
}

main().catch((err: unknown) => {
  // eslint-disable-next-line no-console
  console.error('order-service failed to start', err);
  process.exit(1);
});
