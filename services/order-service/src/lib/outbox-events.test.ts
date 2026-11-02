import { describe, expect, it } from 'vitest';
import { EVENT_TYPES, TOPICS, createId } from '@seatlock/shared';
import type { OutboxRow } from '../db/types.js';
import { buildOutboxRecord, toKafkaMessage, topicForEvent } from './outbox-events.js';

describe('topicForEvent', () => {
  it('sends PaymentRequested to payments.events', () => {
    expect(topicForEvent(EVENT_TYPES.PaymentRequested)).toBe(TOPICS.PAYMENTS);
  });

  it.each([
    EVENT_TYPES.OrderCreated,
    EVENT_TYPES.OrderConfirmed,
    EVENT_TYPES.OrderFailed,
    EVENT_TYPES.OrderExpired,
  ])('sends %s to orders.events', (type) => {
    expect(topicForEvent(type)).toBe(TOPICS.ORDERS);
  });
});

describe('buildOutboxRecord', () => {
  const orderId = createId();
  const holdId = createId();
  const eventId = createId();

  it('uses the envelope eventId as the row id and the orderId as partition key', () => {
    const record = buildOutboxRecord({
      type: EVENT_TYPES.OrderExpired,
      orderId,
      correlationId: 'corr-1',
      payload: { orderId, holdId, eventId },
    });

    expect(record.id).toMatch(/^[0-9a-f-]{36}$/);
    expect(record).toMatchObject({
      aggregateType: 'order',
      aggregateId: orderId,
      eventType: 'OrderExpired',
      headers: { correlationId: 'corr-1', producer: 'order-service', partitionKey: orderId },
    });
  });

  it('refuses to queue a payload that violates the shared schema', () => {
    expect(() =>
      buildOutboxRecord({
        type: EVENT_TYPES.OrderFailed,
        orderId,
        correlationId: 'c',
        payload: { orderId, holdId, eventId, reason: '' }, // reason must be non-empty
      }),
    ).toThrow();
  });
});

describe('toKafkaMessage', () => {
  function expiredRow(): OutboxRow {
    const orderId = createId();
    const holdId = createId();
    const eventId = createId();
    const record = buildOutboxRecord({
      type: EVENT_TYPES.OrderExpired,
      orderId,
      correlationId: 'corr-9',
      payload: { orderId, holdId, eventId },
    });
    return { ...record, published: false, createdAt: new Date(), publishedAt: null };
  }

  it('rebuilds the envelope that was captured at write time', () => {
    const row = expiredRow();
    const msg = toKafkaMessage(row);

    expect(msg.topic).toBe(TOPICS.ORDERS);
    expect(msg.key).toBe(row.aggregateId);
    expect(msg.headers).toEqual({
      'event-type': 'OrderExpired',
      'event-id': row.id,
      'correlation-id': 'corr-9',
    });

    expect(JSON.parse(msg.value)).toEqual({
      eventId: row.id,
      type: 'OrderExpired',
      version: 1,
      occurredAt: row.headers.occurredAt,
      correlationId: 'corr-9',
      producer: 'order-service',
      payload: row.payload,
    });
  });

  it('falls back to the aggregate id when headers are missing', () => {
    const row: OutboxRow = { ...expiredRow(), headers: {} };
    const msg = toKafkaMessage(row);

    expect(msg.key).toBe(row.aggregateId);
    expect(JSON.parse(msg.value).correlationId).toBe(row.aggregateId);
  });
});
