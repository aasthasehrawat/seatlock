import {
  EVENT_SCHEMAS,
  EVENT_TYPES,
  TOPICS,
  createEnvelope,
  type EventEnvelope,
  type EventPayloadMap,
  type EventType,
  type Topic,
} from '@seatlock/shared';
import type { ZodTypeAny } from 'zod';
import type { OutboxRecord, OutboxRow } from '../db/types.js';

export const PRODUCER_NAME = 'order-service';
export const ORDER_AGGREGATE = 'order';

/** Events this service is allowed to write to its outbox. */
export type OrderEventType =
  | typeof EVENT_TYPES.OrderCreated
  | typeof EVENT_TYPES.PaymentRequested
  | typeof EVENT_TYPES.OrderConfirmed
  | typeof EVENT_TYPES.OrderFailed
  | typeof EVENT_TYPES.OrderExpired;

export interface BuildOutboxInput<T extends OrderEventType> {
  type: T;
  payload: EventPayloadMap[T];
  orderId: string;
  correlationId: string;
}

/**
 * Build an outbox row for an order event.
 *
 * The payload is validated against the shared Zod schema BEFORE it is written: a malformed
 * event would otherwise be committed to the DB and then poison every consumer downstream.
 * Failing here aborts the surrounding transaction, so bad data never leaves the service.
 *
 * `id` is the envelope eventId, so the same value is the outbox PK and the consumers' dedupe key.
 * Partition key is the orderId, so all events of one order stay ordered on one partition.
 */
export function buildOutboxRecord<T extends OrderEventType>(input: BuildOutboxInput<T>): OutboxRecord {
  const payload = (EVENT_SCHEMAS[input.type] as ZodTypeAny).parse(input.payload) as unknown;

  const envelope = createEnvelope({
    type: input.type,
    payload,
    producer: PRODUCER_NAME,
    correlationId: input.correlationId,
  });

  return {
    id: envelope.eventId,
    aggregateType: ORDER_AGGREGATE,
    aggregateId: input.orderId,
    eventType: input.type,
    payload,
    headers: {
      correlationId: envelope.correlationId,
      producer: envelope.producer,
      version: envelope.version,
      occurredAt: envelope.occurredAt,
      partitionKey: input.orderId,
    },
  };
}

/** PaymentRequested is a command for the payment service; all other order events are facts. */
export function topicForEvent(type: EventType): Topic {
  return type === EVENT_TYPES.PaymentRequested ? TOPICS.PAYMENTS : TOPICS.ORDERS;
}

export interface KafkaOutboundMessage {
  topic: Topic;
  key: string;
  value: string;
  headers: Record<string, string>;
}

/** Rebuild the exact envelope that was captured at write time and address it to its topic. */
export function toKafkaMessage(row: OutboxRow): KafkaOutboundMessage {
  const correlationId = row.headers.correlationId ?? row.aggregateId;

  const envelope: EventEnvelope = {
    eventId: row.id,
    type: row.eventType,
    version: row.headers.version ?? 1,
    occurredAt: row.headers.occurredAt ?? row.createdAt.toISOString(),
    correlationId,
    producer: row.headers.producer ?? PRODUCER_NAME,
    payload: row.payload,
  };

  return {
    topic: topicForEvent(row.eventType),
    key: row.headers.partitionKey ?? row.aggregateId,
    value: JSON.stringify(envelope),
    headers: {
      'event-type': row.eventType,
      'event-id': row.id,
      'correlation-id': correlationId,
    },
  };
}
