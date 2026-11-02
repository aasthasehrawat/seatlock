import { Kafka, logLevel, type Producer, type logCreator } from 'kafkajs';
import { TOPICS } from '@seatlock/shared';
import type { Config } from '../config.js';
import type { Logger } from './logger.js';

/** Route KafkaJS's internal logs through our pino logger so everything is one JSON stream. */
function pinoLogCreator(logger: Logger): logCreator {
  return () =>
    ({ namespace, level, log }) => {
      const { message, timestamp: _timestamp, logger: _logger, ...extra } = log;
      const fields = { kafkaNamespace: namespace, ...extra };
      switch (level) {
        case logLevel.ERROR:
          logger.error(fields, message);
          break;
        case logLevel.WARN:
          logger.warn(fields, message);
          break;
        case logLevel.INFO:
          logger.info(fields, message);
          break;
        default:
          logger.debug(fields, message);
      }
    };
}

export function createKafka(
  config: Pick<Config, 'kafkaBrokers' | 'kafkaClientId'>,
  logger: Logger,
): Kafka {
  return new Kafka({
    clientId: config.kafkaClientId,
    brokers: config.kafkaBrokers,
    logLevel: logLevel.WARN,
    logCreator: pinoLogCreator(logger.child({ component: 'kafkajs' })),
    retry: { initialRetryTime: 300, retries: 8 },
  });
}

export interface ManagedProducer {
  producer: Producer;
  isConnected(): boolean;
}

/**
 * Idempotent producer: the broker de-duplicates retried sends within a session, so a network
 * blip between "send" and "ack" cannot write the same record twice. `maxInFlightRequests: 1`
 * keeps per-partition ordering intact across retries.
 */
export function createProducer(kafka: Kafka): ManagedProducer {
  const producer = kafka.producer({
    idempotent: true,
    maxInFlightRequests: 1,
    allowAutoTopicCreation: true,
  });

  let connected = false;
  producer.on(producer.events.CONNECT, () => {
    connected = true;
  });
  producer.on(producer.events.DISCONNECT, () => {
    connected = false;
  });

  return { producer, isConnected: () => connected };
}

/**
 * Create the topics this service reads or writes if they do not exist yet. Needed where broker
 * auto-creation is disabled. Existing topics are left untouched (partition counts are not altered).
 */
export async function ensureTopics(
  kafka: Kafka,
  config: Pick<Config, 'kafkaTopicPartitions'>,
  logger: Logger,
): Promise<void> {
  const admin = kafka.admin();
  await admin.connect();
  try {
    const created = await admin.createTopics({
      waitForLeaders: true,
      topics: [
        TOPICS.INVENTORY,
        TOPICS.ORDERS,
        TOPICS.PAYMENTS,
        TOPICS.RETRY_5S,
        TOPICS.DLQ,
      ].map((topic) => ({ topic, numPartitions: config.kafkaTopicPartitions })),
    });
    logger.info({ created }, 'kafka topics ensured');
  } finally {
    await admin.disconnect();
  }
}
