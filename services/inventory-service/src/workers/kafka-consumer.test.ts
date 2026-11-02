import { EVENT_TYPES, TOPICS, createEnvelope, createId, type EventType } from '@seatlock/shared';
import { describe, expect, it, vi } from 'vitest';
import { createMockPool, type Responder } from '../test-support/mock-pool.js';
import {
  DLQ_HEADERS,
  RETRY_HEADERS,
  handleMessage,
  type ConsumerDeps,
  type IncomingMessage,
} from './kafka-consumer.js';

const holdId = createId();
const eventId = createId();
const orderId = createId();
const NOW = 1_800_000_000_000;
const CONSUMER = 'inventory-service';

const silentLogger = { info() {}, warn() {}, error() {}, debug() {} } as never;

function envelope(type: EventType, payload: Record<string, unknown>) {
  return createEnvelope({ type, payload, producer: 'order-service', correlationId: 'corr-1' });
}

function message(env: unknown, extra: Partial<IncomingMessage> = {}): IncomingMessage {
  return {
    topic: TOPICS.ORDERS,
    key: Buffer.from(eventId),
    value: Buffer.from(typeof env === 'string' ? env : JSON.stringify(env)),
    ...extra,
  };
}

function setup(responder: Responder) {
  const mock = createMockPool(responder);
  const send = vi.fn(async () => []);
  const sleep = vi.fn(async (_ms: number): Promise<void> => undefined);
  const consumerDeps: ConsumerDeps = {
    deps: { pool: mock.pool, holdTtlSeconds: 300, serviceName: CONSUMER },
    consumerName: CONSUMER,
    producer: async () => ({ send }),
    logger: silentLogger,
    maxAttempts: 3,
    retryDelayMs: 5_000,
    now: () => NOW,
    sleep,
  };
  return { ...mock, consumerDeps, send, sleep };
}

const confirmable: Responder = (sql) => {
  if (sql.startsWith('INSERT INTO processed_events')) return { rowCount: 1 };
  if (sql.startsWith('SELECT id, event_id, quantity, status, order_id FROM holds')) {
    return { rows: [{ id: holdId, event_id: eventId, quantity: 2, status: 'ACTIVE', order_id: null }] };
  }
  if (sql.startsWith('UPDATE holds')) return { rowCount: 1 };
  return undefined;
};

const orderConfirmed = () =>
  envelope(EVENT_TYPES.OrderConfirmed, {
    orderId,
    holdId,
    eventId,
    userId: createId(),
    quantity: 2,
    amountCents: 1000,
    paymentId: createId(),
  });

describe('kafka consumer', () => {
  it('OrderConfirmed -> confirms the hold and records the message as processed (same transaction)', async () => {
    const t = setup(confirmable);

    const outcome = await handleMessage(t.consumerDeps, message(orderConfirmed()));

    expect(outcome).toBe('processed');
    expect(t.has('INSERT INTO processed_events')).toBe(true);
    expect(t.find('UPDATE holds')[0]!.values).toEqual([holdId, orderId]);
    expect(t.calls.filter((c) => c.sql === 'BEGIN')).toHaveLength(1);
    expect(t.calls.filter((c) => c.sql === 'COMMIT')).toHaveLength(1);
    expect(t.send).not.toHaveBeenCalled();
  });

  it('PaymentSucceeded also confirms the hold', async () => {
    const t = setup(confirmable);
    const env = envelope(EVENT_TYPES.PaymentSucceeded, {
      paymentId: createId(),
      orderId,
      holdId,
      amountCents: 1000,
      providerRef: 'ref-1',
    });

    expect(await handleMessage(t.consumerDeps, message(env))).toBe('processed');
    expect(t.find('UPDATE holds')[0]!.values).toEqual([holdId, orderId]);
  });

  it('skips a duplicate message (processed_events conflict) without touching holds', async () => {
    const t = setup((sql) => (sql.startsWith('INSERT INTO processed_events') ? { rowCount: 0 } : undefined));

    expect(await handleMessage(t.consumerDeps, message(orderConfirmed()))).toBe('duplicate');
    expect(t.has('FROM holds')).toBe(false);
    expect(t.send).not.toHaveBeenCalled();
  });

  it.each([
    [EVENT_TYPES.PaymentFailed, { paymentId: createId(), orderId, holdId, amountCents: 1, reason: 'declined' }, 'RELEASED'],
    [EVENT_TYPES.OrderFailed, { orderId, holdId, eventId, reason: 'cancelled' }, 'RELEASED'],
    [EVENT_TYPES.OrderExpired, { orderId, holdId, eventId }, 'EXPIRED'],
  ] as const)('%s -> releases the hold', async (type, payload, expectedState) => {
    const t = setup((sql) => {
      if (sql.startsWith('INSERT INTO processed_events')) return { rowCount: 1 };
      if (sql.startsWith('UPDATE holds')) return { rows: [{ event_id: eventId, quantity: 2 }] };
      if (sql.startsWith('UPDATE events SET available = available +')) {
        return { rows: [{ available: 8, total_tickets: 10 }] };
      }
      if (sql.startsWith('SELECT available FROM events')) return { rows: [{ available: 8 }] };
      return undefined;
    });

    expect(await handleMessage(t.consumerDeps, message(envelope(type, { ...payload })))).toBe('processed');
    expect(t.find('UPDATE holds')[0]!.values).toEqual([holdId, expectedState]);
  });

  it('ignores event types it does not care about, without recording them', async () => {
    const t = setup(() => undefined);
    const env = envelope(EVENT_TYPES.OrderCreated, {
      orderId,
      holdId,
      eventId,
      userId: createId(),
      quantity: 1,
      amountCents: 1,
    });

    expect(await handleMessage(t.consumerDeps, message(env))).toBe('ignored');
    expect(t.calls).toHaveLength(0);
  });

  it('on a transient failure republishes the ORIGINAL payload to retry.5s with attempt metadata', async () => {
    const t = setup((sql) => {
      if (sql.startsWith('INSERT INTO processed_events')) throw new Error('connection reset');
      return undefined;
    });
    const msg = message(orderConfirmed());

    const outcome = await handleMessage(t.consumerDeps, msg);

    expect(outcome).toBe('retried');
    expect(t.has('ROLLBACK')).toBe(true); // marker rolled back, so the retry is not seen as a duplicate
    expect(t.send).toHaveBeenCalledTimes(1);
    const record = (t.send.mock.calls[0] as unknown as [{ topic: string; messages: Array<Record<string, any>> }])[0];
    expect(record.topic).toBe(TOPICS.RETRY_5S);
    const sent = record.messages[0]!;
    expect(sent.value).toBe(msg.value); // byte-for-byte original
    expect(sent.key).toBe(msg.key);
    expect(sent.headers).toMatchObject({
      [RETRY_HEADERS.attempt]: '1',
      [RETRY_HEADERS.consumer]: CONSUMER,
      [RETRY_HEADERS.originalTopic]: TOPICS.ORDERS,
      [RETRY_HEADERS.notBefore]: String(NOW + 5_000),
      [RETRY_HEADERS.lastError]: 'connection reset',
    });
  });

  it('dead-letters a retried message once maxAttempts is exhausted', async () => {
    const t = setup((sql) => {
      if (sql.startsWith('INSERT INTO processed_events')) throw new Error('still down');
      return undefined;
    });
    const msg = message(orderConfirmed(), {
      topic: TOPICS.RETRY_5S,
      headers: {
        [RETRY_HEADERS.attempt]: '2', // 2 failures so far; this is attempt 3 of 3
        [RETRY_HEADERS.consumer]: CONSUMER,
        [RETRY_HEADERS.originalTopic]: TOPICS.PAYMENTS,
        [RETRY_HEADERS.notBefore]: String(NOW - 1),
      },
    });

    expect(await handleMessage(t.consumerDeps, msg)).toBe('dead-lettered');
    const record = (t.send.mock.calls[0] as unknown as [{ topic: string; messages: Array<Record<string, any>> }])[0];
    expect(record.topic).toBe(TOPICS.DLQ);
    expect(record.messages[0]!.headers).toMatchObject({
      [DLQ_HEADERS.reason]: 'MAX_ATTEMPTS_EXCEEDED',
      [DLQ_HEADERS.originalTopic]: TOPICS.PAYMENTS,
      [DLQ_HEADERS.attempts]: '3',
    });
  });

  it('waits for the not-before time of a retry message before processing it', async () => {
    let clock = NOW;
    const t = setup(confirmable);
    t.consumerDeps.now = () => clock;
    t.sleep.mockImplementation(async (ms: number) => {
      clock += ms;
    });
    const msg = message(orderConfirmed(), {
      topic: TOPICS.RETRY_5S,
      headers: {
        [RETRY_HEADERS.attempt]: '1',
        [RETRY_HEADERS.consumer]: CONSUMER,
        [RETRY_HEADERS.originalTopic]: TOPICS.ORDERS,
        [RETRY_HEADERS.notBefore]: String(NOW + 2_500),
      },
      heartbeat: vi.fn(async () => undefined),
    });

    expect(await handleMessage(t.consumerDeps, msg)).toBe('processed');
    expect(t.sleep).toHaveBeenCalled();
    expect(clock).toBeGreaterThanOrEqual(NOW + 2_500);
    expect(msg.heartbeat).toHaveBeenCalled();
  });

  it("ignores another service's messages on the shared retry topic", async () => {
    const t = setup(() => undefined);
    const msg = message(orderConfirmed(), {
      topic: TOPICS.RETRY_5S,
      headers: { [RETRY_HEADERS.consumer]: 'order-service' },
    });

    expect(await handleMessage(t.consumerDeps, msg)).toBe('ignored');
    expect(t.calls).toHaveLength(0);
    expect(t.send).not.toHaveBeenCalled();
  });

  it('dead-letters malformed JSON immediately (retrying cannot help)', async () => {
    const t = setup(() => undefined);

    expect(await handleMessage(t.consumerDeps, message('{not json'))).toBe('dead-lettered');
    const record = (t.send.mock.calls[0] as unknown as [{ topic: string; messages: Array<Record<string, any>> }])[0];
    expect(record.topic).toBe(TOPICS.DLQ);
    expect(record.messages[0]!.headers[DLQ_HEADERS.reason]).toBe('INVALID_JSON');
  });

  it('dead-letters when the business rule says no (unknown hold) instead of retrying forever', async () => {
    const t = setup((sql) => (sql.startsWith('INSERT INTO processed_events') ? { rowCount: 1 } : undefined));

    expect(await handleMessage(t.consumerDeps, message(orderConfirmed()))).toBe('dead-lettered');
    const record = (t.send.mock.calls[0] as unknown as [{ topic: string; messages: Array<Record<string, any>> }])[0];
    expect(record.topic).toBe(TOPICS.DLQ);
    expect(record.messages[0]!.headers[DLQ_HEADERS.reason]).toBe('NOT_FOUND');
  });
});
