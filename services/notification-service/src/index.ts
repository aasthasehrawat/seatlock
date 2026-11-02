import './lib/load-env.js';
import { buildApp } from './app.js';
import { loadConfig } from './config.js';
import { migrate } from './db/migrate.js';
import { createPool } from './db/pool.js';
import { createKafka, createProducer } from './lib/kafka.js';
import { createLogger } from './lib/logger.js';
import { createMockSender } from './notifications/mock-sender.js';
import { PgNotificationRepository } from './notifications/pg-repository.js';
import { startNotificationConsumer } from './workers/notification-consumer.js';
import type { ProcessorDeps } from './workers/notification-processor.js';

async function main(): Promise<void> {
  const config = loadConfig();
  const logger = createLogger({ level: config.logLevel, name: config.serviceName });

  const pool = createPool(config.databaseUrl, logger);
  await migrate(pool, logger);
  const repo = new PgNotificationRepository(pool);

  const kafka = createKafka({ brokers: config.kafkaBrokers, clientId: config.kafkaClientId });
  const producer = createProducer(kafka);
  await producer.connect();
  let kafkaReady = false;

  const deps: ProcessorDeps = {
    repo,
    sender: createMockSender(logger),
    logger,
    channel: 'EMAIL',
    userLookupRetries: config.userLookupRetries,
    userLookupDelayMs: config.userLookupDelayMs,
  };

  const consumer = await startNotificationConsumer({
    kafka,
    producer,
    groupId: config.kafkaGroupId,
    deps,
    logger,
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
    readinessChecks: {
      database: async () => {
        await pool.query('SELECT 1');
        return true;
      },
      kafka: async () => kafkaReady,
    },
  });

  await app.listen({ port: config.port, host: config.host });
  logger.info({ port: config.port }, 'notification-service started');

  let shuttingDown = false;
  const shutdown = async (signal: string): Promise<void> => {
    if (shuttingDown) return;
    shuttingDown = true;
    logger.info({ signal }, 'Shutting down');
    try {
      await app.close();
      await consumer.disconnect();
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
  console.error('notification-service failed to start', err);
  process.exit(1);
});
