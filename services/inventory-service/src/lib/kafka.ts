import { Kafka, logLevel, type Consumer, type LogEntry, type Producer } from 'kafkajs';
import { TOPICS } from '@seatlock/shared';
import { config } from '../config.js';
import { logger } from './logger.js';

/** Bridge kafkajs's internal logging into our structured pino logger. */
const kafkaLogCreator = () => (entry: LogEntry) => {
  const { namespace, level, log } = entry;
  const { message, ...extra } = log;
  const fields = { kafkaNamespace: namespace, ...extra };
  switch (level) {
    case logLevel.ERROR:
    case logLevel.NOTHING:
      logger.error(fields, `kafka: ${message}`);
      break;
    case logLevel.WARN:
      logger.warn(fields, `kafka: ${message}`);
      break;
    case logLevel.INFO:
      logger.info(fields, `kafka: ${message}`);
      break;
    default:
      logger.debug(fields, `kafka: ${message}`);
  }
};

export function createKafka(): Kafka {
  return new Kafka({
    clientId: config.serviceName,
    brokers: config.kafkaBrokers,
    logLevel: logLevel.WARN,
    logCreator: kafkaLogCreator,
    retry: { initialRetryTime: 300, retries: 8 },
  });
}

let kafka: Kafka | undefined;
let producer: Producer | undefined;
let producerConnected = false;

export function getKafka(): Kafka {
  kafka ??= createKafka();
  return kafka;
}

/**
 * Lazily connected, idempotent producer.
 *
 * `idempotent: true` makes the broker de-duplicate producer-side retries (a network
 * timeout followed by a retry cannot write the same record twice within one producer
 * session). It does NOT protect against the relay crashing after send but before marking
 * the outbox row published; that replay is handled by consumers via `processed_events`.
 *
 * Connecting lazily means the HTTP API can start (and keep accepting reservations into
 * the outbox) even while Kafka is down; the relay simply retries until it is back.
 */
export async function getProducer(): Promise<Producer> {
  producer ??= getKafka().producer({
    idempotent: true,
    maxInFlightRequests: 1,
    allowAutoTopicCreation: true,
  });
  if (!producerConnected) {
    await producer.connect();
    producerConnected = true;
  }
  return producer;
}

export async function disconnectProducer(): Promise<void> {
  if (producer && producerConnected) {
    producerConnected = false;
    await producer.disconnect().catch((err: Error) => logger.warn({ err: err.message }, 'producer disconnect failed'));
  }
}

export function createConsumer(groupId: string): Consumer {
  return getKafka().consumer({
    groupId,
    sessionTimeout: 30_000,
    heartbeatInterval: 3_000,
    allowAutoTopicCreation: true,
  });
}

/**
 * Best-effort topic creation so a fresh broker works out of the box.
 * `inventory.events` is keyed by concert id; several partitions let different concerts
 * proceed in parallel while each concert keeps strict ordering within its partition.
 * Failure is non-fatal: brokers with auto-create enabled will create topics on first use.
 */
export async function ensureTopics(): Promise<void> {
  const admin = getKafka().admin();
  try {
    await admin.connect();
    await admin.createTopics({
      waitForLeaders: true,
      topics: [
        { topic: TOPICS.INVENTORY, numPartitions: 6 },
        { topic: TOPICS.ORDERS, numPartitions: 6 },
        { topic: TOPICS.PAYMENTS, numPartitions: 6 },
        { topic: TOPICS.RETRY_5S, numPartitions: 3 },
        { topic: TOPICS.DLQ, numPartitions: 1 },
      ],
    });
  } catch (err) {
    logger.warn({ err: (err as Error).message }, 'could not ensure kafka topics (continuing)');
  } finally {
    await admin.disconnect().catch(() => undefined);
  }
}
