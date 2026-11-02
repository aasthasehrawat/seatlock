import './lib/load-env.js';
import type { Consumer } from 'kafkajs';
import { buildApp } from './app.js';
import { loadConfig } from './config.js';
import { migrate } from './db/migrate.js';
import { createPool } from './db/pool.js';
import { createKafka, createProducer } from './lib/kafka.js';
import { createLogger } from './lib/logger.js';
import { PgPaymentRepository } from './payments/pg-repository.js';
import { startPaymentConsumer } from './workers/payment-consumer.js';
import { OutboxRelay } from './workers/outbox-relay.js';
import type { ProcessorDeps } from './workers/payment-processor.js';

async function main(): Promise<void> {
  const config = loadConfig();
  const logger = createLogger({ level: config.logLevel, name: config.serviceName });

  const pool = createPool(config.databaseUrl, logger);
  await migrate(pool, logger);
  const repo = new PgPaymentRepository(pool);

  const kafka = createKafka({ brokers: config.kafkaBrokers, clientId: config.kafkaClientId });
  const producer = createProducer(kafka);
  await producer.connect();
  let kafkaReady = false;

  const deps: ProcessorDeps = {
    repo,
    logger,
    producerName: config.serviceName,
    failureRate: config.paymentFailureRate,
    latencyMs: config.paymentLatencyMs,
  };

  const relay = new OutboxRelay({
    pool,
    producer,
    logger,
    pollIntervalMs: config.outboxPollIntervalMs,
    batchSize: config.outboxBatchSize,
  });
  relay.start();

  const consumer: Consumer = await startPaymentConsumer({
    kafka,
    producer,
    groupId: config.kafkaGroupId,
    deps,
    logger,
    concurrency: config.consumerConcurrency,
    serviceName: config.serviceName,
  });
  kafkaReady = true;
  consumer.on(consumer.events.CRASH, ({ payload }) => {
    kafkaReady = false;
    logger.error({ err: payload.error, restart: payload.restart }, 'Kafka consumer crashed');
  });
  consumer.on(consumer.events.CONNECT, () => {
    kafkaReady = true;
  });
  consumer.on(consumer.events.DISCONNECT, () => {
    kafkaReady = false;
  });

  const app = await buildApp({
    logger,
    repo,
    producerName: config.serviceName,
    enableSimulateRoute: config.enableSimulateRoute,
    readinessChecks: {
      database: async () => {
        await pool.query('SELECT 1');
        return true;
      },
      kafka: async () => kafkaReady,
    },
  });

  await app.listen({ port: config.port, host: config.host });
  logger.info(
    {
      port: config.port,
      failureRate: config.paymentFailureRate,
      latencyMs: config.paymentLatencyMs,
      simulateRoute: config.enableSimulateRoute,
    },
    'payment-service started',
  );

  let shuttingDown = false;
  const shutdown = async (signal: string): Promise<void> => {
    if (shuttingDown) return;
    shuttingDown = true;
    logger.info({ signal }, 'Shutting down');
    try {
      await app.close();
      await consumer.disconnect();
      await relay.stop();
      await producer.disconnect();
      await pool.end();
    } catch (err) {
      logger.error({ err }, 'Error during shutdown');
      process.exitCode = 1;
    }
    process.exit();
  };
  process.on('SIGINT', () => void shutdown('SIGINT'));
  process.on('SIGTERM', () => void shutdown('SIGTERM'));
}

main().catch((err) => {
  console.error('payment-service failed to start', err);
  process.exit(1);
});
