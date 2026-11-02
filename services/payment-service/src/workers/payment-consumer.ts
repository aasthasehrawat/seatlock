import type { Kafka, Consumer, Producer } from 'kafkajs';
import { TOPICS } from '@seatlock/shared';
import { sendToDlq } from '../lib/kafka.js';
import type { Logger } from '../lib/logger.js';
import { handlePaymentMessage, type ProcessorDeps } from './payment-processor.js';

export interface PaymentConsumerOptions {
  kafka: Kafka;
  producer: Producer;
  groupId: string;
  deps: ProcessorDeps;
  logger: Logger;
  /** How many partitions are processed in parallel (each message sleeps for the simulated latency). */
  concurrency: number;
  serviceName: string;
}

/**
 * Consume PaymentRequested events from payments.events.
 * - Malformed messages go to the DLQ so they can't block the partition.
 * - Processing errors are thrown so kafkajs retries the message.
 */
export async function startPaymentConsumer(options: PaymentConsumerOptions): Promise<Consumer> {
  const { kafka, producer, groupId, deps, logger, concurrency, serviceName } = options;

  const consumer = kafka.consumer({ groupId, allowAutoTopicCreation: true });
  await consumer.connect();
  await consumer.subscribe({ topic: TOPICS.PAYMENTS, fromBeginning: true });

  await consumer.run({
    partitionsConsumedConcurrently: concurrency,
    eachMessage: async ({ topic, message }) => {
      const outcome = await handlePaymentMessage(deps, message.value);

      switch (outcome.kind) {
        case 'invalid':
          logger.warn({ topic, reason: outcome.reason }, 'Invalid message; sending to DLQ');
          await sendToDlq(producer, {
            sourceTopic: topic,
            key: message.key,
            value: message.value,
            reason: outcome.reason,
            service: serviceName,
          });
          break;
        case 'ignored':
          logger.debug({ type: outcome.type }, 'Ignoring event not meant for payment-service');
          break;
        case 'handled':
          break;
      }
    },
  });

  logger.info({ topic: TOPICS.PAYMENTS, groupId }, 'Payment consumer started');
  return consumer;
}
