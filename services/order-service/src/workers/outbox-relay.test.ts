import { describe, expect, it, vi } from 'vitest';
import { TOPICS } from '@seatlock/shared';
import {
  createHarness,
  meta,
  paymentSucceeded,
  seedPendingOrder,
  silentLogger,
  type Harness,
} from '../testing/fixtures.js';
import { OutboxRelay } from './outbox-relay.js';

type SendBatch = (args: {
  acks?: number;
  topicMessages: Array<{
    topic: string;
    messages: Array<{ key: string; value: string; headers: Record<string, string> }>;
  }>;
}) => Promise<unknown>;

function makeRelay(h: Harness, sendBatch: SendBatch, batchSize = 100) {
  return new OutboxRelay({
    uow: h.uow,
    // The relay only needs sendBatch; cast keeps the fake tiny.
    producer: { sendBatch } as never,
    logger: silentLogger,
    pollIntervalMs: 10,
    batchSize,
    retentionHours: 24,
    idempotencyTtlHours: 24,
  });
}

describe('OutboxRelay.runOnce', () => {
  it('publishes unpublished rows and marks them published', async () => {
    const h = createHarness();
    const order = await seedPendingOrder(h);
    const sendBatch = vi.fn<SendBatch>().mockResolvedValue([]);

    const count = await makeRelay(h, sendBatch).runOnce();

    expect(count).toBe(1);
    expect(sendBatch).toHaveBeenCalledTimes(1);
    const { topicMessages, acks } = sendBatch.mock.calls[0]![0];
    expect(acks).toBe(-1);
    expect(topicMessages).toHaveLength(1);
    expect(topicMessages[0]!.topic).toBe(TOPICS.ORDERS);
    expect(topicMessages[0]!.messages[0]!.key).toBe(order.id);
    expect(h.uow.outbox.every((r) => r.published && r.publishedAt)).toBe(true);
  });

  it('routes PaymentRequested to payments.events and the rest to orders.events, in order', async () => {
    const h = createHarness();
    const order = await seedPendingOrder(h); // OrderCreated
    await h.service.initiatePayment({ orderId: order.id, idempotencyKey: 'k' }); // PaymentRequested
    await h.service.onPaymentSucceeded(meta(), paymentSucceeded(h.uow.getOrder(order.id)!)); // OrderConfirmed
    const sendBatch = vi.fn<SendBatch>().mockResolvedValue([]);

    const count = await makeRelay(h, sendBatch).runOnce();

    expect(count).toBe(3);
    const { topicMessages } = sendBatch.mock.calls[0]![0];
    const byTopic = Object.fromEntries(
      topicMessages.map((t) => [
        t.topic,
        t.messages.map((m) => JSON.parse(m.value).type as string),
      ]),
    );
    expect(byTopic).toEqual({
      [TOPICS.ORDERS]: ['OrderCreated', 'OrderConfirmed'],
      [TOPICS.PAYMENTS]: ['PaymentRequested'],
    });
  });

  it('does not re-send rows that were already published', async () => {
    const h = createHarness();
    await seedPendingOrder(h);
    const sendBatch = vi.fn<SendBatch>().mockResolvedValue([]);
    const relay = makeRelay(h, sendBatch);

    await relay.runOnce();
    const second = await relay.runOnce();

    expect(second).toBe(0);
    expect(sendBatch).toHaveBeenCalledTimes(1);
  });

  it('leaves rows unpublished when Kafka fails, so the next poll retries them', async () => {
    const h = createHarness();
    await seedPendingOrder(h);
    const sendBatch = vi
      .fn<SendBatch>()
      .mockRejectedValueOnce(new Error('broker unavailable'))
      .mockResolvedValue([]);
    const relay = makeRelay(h, sendBatch);

    await expect(relay.runOnce()).rejects.toThrow('broker unavailable');
    expect(h.uow.outbox.every((r) => !r.published)).toBe(true);

    expect(await relay.runOnce()).toBe(1);
    expect(h.uow.outbox.every((r) => r.published)).toBe(true);
  });

  it('respects the batch size', async () => {
    const h = createHarness();
    await seedPendingOrder(h);
    await seedPendingOrder(h);
    await seedPendingOrder(h);
    const sendBatch = vi.fn<SendBatch>().mockResolvedValue([]);
    const relay = makeRelay(h, sendBatch, 2);

    expect(await relay.runOnce()).toBe(2);
    expect(await relay.runOnce()).toBe(1);
    expect(await relay.runOnce()).toBe(0);
  });
});

describe('OutboxRelay loop', () => {
  it('drains the outbox in the background and stops cleanly', async () => {
    const h = createHarness();
    await seedPendingOrder(h);
    const sendBatch = vi.fn<SendBatch>().mockResolvedValue([]);
    const relay = makeRelay(h, sendBatch);

    relay.start();
    await vi.waitFor(() => expect(h.uow.outbox.every((r) => r.published)).toBe(true));
    expect(relay.isHealthy()).toBe(true);

    await relay.stop();
    expect(relay.isHealthy()).toBe(false);
  });
});
