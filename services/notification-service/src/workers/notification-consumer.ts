import type { Consumer, Kafka, Producer } from 'kafkajs';
import { TOPICS } from '@seatlock/shared';
import { sendToDlq } from '../lib/kafka.js';
import type { Logger } from '../lib/logger.js';
import { handleMessage, type ProcessorDeps } from './notification-processor.js';

export const CONSUMED_TOPICS = [TOPICS.INVENTORY, TOPICS.ORDERS, TOPICS.PAYMENTS] as const;

export interface NotificationConsumerOptions {
  kafka: Kafka;
  producer: Producer;
  groupId: string;
  deps: ProcessorDeps;
  logger: Logger;
  serviceName: string;
}

/**
 * Consume inventory.events, orders.events and payments.events.
 * - Malformed or unresolvable messages go to the DLQ so they can't block a partition.
 * - Unexpected errors (e.g. DB down) are thrown so kafkajs retries the message.
 */
export async function startNotificationConsumer(options: NotificationConsumerOptions): Promise<Consumer> {
  const { kafka, producer, groupId, deps, logger, serviceName } = options;

  const consumer = kafka.consumer({ groupId, allowAutoTopicCreation: true });
  await consumer.connect();
  for (const topic of CONSUMED_TOPICS) {
    await consumer.subscribe({ topic, fromBeginning: true });
  }

  await consumer.run({
    eachMessage: async ({ topic, message }) => {
      const result = await handleMessage(deps, message.value);

      switch (result.kind) {
        case 'invalid':
        case 'unresolved': {
          const reason = result.kind === 'invalid' ? result.reason : `No owner known for hold ${result.holdId}`;
          logger.warn({ topic, reason }, 'Message could not be handled; sending to DLQ');
          await sendToDlq(producer, {
            sourceTopic: topic,
            key: message.key,
            value: message.value,
            reason,
            service: serviceName,
          });
          break;
        }
        case 'ignored':
        case 'duplicate':
        case 'recorded':
        case 'notified':
          break;
      }
    },
  });

  logger.info({ topics: CONSUMED_TOPICS, groupId }, 'Notification consumer started');
  return consumer;
}
