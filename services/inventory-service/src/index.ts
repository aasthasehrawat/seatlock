import './lib/load-env.js';
import { buildApp } from './app.js';
import { config } from './config.js';
import type { AppContext } from './context.js';
import { closePool, getPool } from './db/pool.js';
import { disconnectProducer, ensureTopics, getProducer } from './lib/kafka.js';
import { logger } from './lib/logger.js';
import { connectRedis, disconnectRedis, getRedis } from './lib/redis.js';
import type { ServiceDeps } from './services/deps.js';
import { createHoldExpiryWorker } from './workers/hold-expiry.js';
import { createKafkaConsumerWorker } from './workers/kafka-consumer.js';
import { createOutboxRelay } from './workers/outbox-relay.js';

const SHUTDOWN_TIMEOUT_MS = 15_000;

async function main(): Promise<void> {
  const pool = getPool();
  const redis = getRedis();
  await connectRedis(redis); // best effort: the service runs without Redis

  const services: ServiceDeps = {
    pool,
    holdTtlSeconds: config.holdTtlSeconds,
    serviceName: config.serviceName,
  };
  const ctx: AppContext = { config, pool, redis, services, logger };

  // Fail fast if Postgres is unreachable or misconfigured: no point serving traffic.
  await pool.query('SELECT 1');

  const app = await buildApp(ctx);

  // Kafka-facing workers. None of them require Kafka to be up at boot:
  // the relay/consumer retry on their own, and reservations keep landing in the outbox meanwhile.
  void ensureTopics();

  const outboxRelay = createOutboxRelay({
    pool,
    producer: getProducer,
    batchSize: config.outbox.batchSize,
    intervalMs: config.outbox.pollIntervalMs,
    logger,
  });
  const holdExpiry = createHoldExpiryWorker({
    deps: services,
    pool,
    batchSize: config.holdExpiry.batchSize,
    intervalMs: config.holdExpiry.intervalMs,
    logger,
  });
  const consumer = createKafkaConsumerWorker({
    deps: services,
    consumerName: config.serviceName,
    producer: getProducer,
    logger,
    maxAttempts: config.consumerMaxAttempts,
  });

  await app.listen({ port: config.port, host: '0.0.0.0' });

  outboxRelay.start();
  holdExpiry.start();
  consumer.start();
  logger.info({ port: config.port }, 'inventory-service started');

  // ---- Graceful shutdown -------------------------------------------------------------------
  // Order matters:
  //   1. stop accepting HTTP (finish in-flight requests, so their transactions commit)
  //   2. stop pollers/consumer (let the current batch finish; unfinished work stays in Postgres
  //      and is picked up by the next instance; the outbox and processed_events make this safe)
  //   3. close Kafka producer, Redis, then the Postgres pool last (everything above uses it)
  let shuttingDown = false;
  const shutdown = async (signal: string): Promise<void> => {
    if (shuttingDown) return;
    shuttingDown = true;
    logger.info({ signal }, 'shutting down');

    const forceExit = setTimeout(() => {
      logger.error('graceful shutdown timed out; forcing exit');
      process.exit(1);
    }, SHUTDOWN_TIMEOUT_MS);
    forceExit.unref();

    try {
      await app.close();
      await Promise.all([outboxRelay.stop(), holdExpiry.stop(), consumer.stop()]);
      await disconnectProducer();
      await disconnectRedis(redis);
      await closePool();
      logger.info('shutdown complete');
      process.exit(0);
    } catch (err) {
      logger.error({ err }, 'error during shutdown');
      process.exit(1);
    }
  };

  process.on('SIGTERM', () => void shutdown('SIGTERM'));
  process.on('SIGINT', () => void shutdown('SIGINT'));
}

process.on('unhandledRejection', (reason) => {
  logger.error({ err: reason }, 'unhandled promise rejection');
});

main().catch((err) => {
  logger.fatal({ err }, 'failed to start inventory-service');
  process.exit(1);
});
