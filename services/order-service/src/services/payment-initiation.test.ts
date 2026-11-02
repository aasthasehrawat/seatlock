import { describe, expect, it } from 'vitest';
import { IdempotencyConflictError, NotFoundError, createId } from '@seatlock/shared';
import { OrderNotPayableError, PaymentAlreadyInitiatedError } from '../lib/errors.js';
import {
  createHarness,
  holdExpired,
  meta,
  paymentSucceeded,
  seedOrderWithPayment,
  seedPendingOrder,
} from '../testing/fixtures.js';

describe('initiatePayment', () => {
  it('allocates a payment id, links it to the order and queues PaymentRequested', async () => {
    const h = createHarness();
    const order = await seedPendingOrder(h, { quantity: 4, unitPriceCents: 1250 });

    const result = await h.service.initiatePayment({
      orderId: order.id,
      idempotencyKey: 'key-1',
      correlationId: 'corr-pay',
    });

    expect(result.statusCode).toBe(202);
    expect(result.replayed).toBe(false);
    expect(result.body).toMatchObject({
      orderId: order.id,
      status: 'PENDING_PAYMENT', // payment outcome is asynchronous
      paymentStatus: 'REQUESTED',
      amountCents: 5000,
      currency: 'USD',
    });

    const paymentId = result.body.paymentId;
    expect(h.uow.getOrder(order.id)!.paymentId).toBe(paymentId);

    const [row] = h.uow.outboxOfType('PaymentRequested');
    expect(row).toMatchObject({ aggregateId: order.id, published: false });
    expect(row!.payload).toEqual({
      paymentId,
      orderId: order.id,
      holdId: order.holdId,
      userId: order.userId,
      amountCents: 5000,
      currency: 'USD',
    });
    expect(row!.headers).toMatchObject({ correlationId: 'corr-pay', partitionKey: order.id });
  });

  it('honours an explicit currency, normalised to upper case', async () => {
    const h = createHarness();
    const order = await seedPendingOrder(h);

    const result = await h.service.initiatePayment({
      orderId: order.id,
      idempotencyKey: 'key-eur',
      currency: 'eur',
    });

    expect(result.body.currency).toBe('EUR');
    expect(h.uow.outboxOfType('PaymentRequested')[0]!.payload).toMatchObject({ currency: 'EUR' });
  });

  describe('idempotency', () => {
    it('replays the stored response for the same key and request, with no new side effects', async () => {
      const h = createHarness();
      const order = await seedPendingOrder(h);

      const first = await h.service.initiatePayment({ orderId: order.id, idempotencyKey: 'key-1' });
      const second = await h.service.initiatePayment({ orderId: order.id, idempotencyKey: 'key-1' });

      expect(second.replayed).toBe(true);
      expect(second.statusCode).toBe(first.statusCode);
      expect(second.body).toEqual(first.body);
      expect(h.uow.outboxOfType('PaymentRequested')).toHaveLength(1);
      expect(h.uow.idempotencyCount).toBe(1);
    });

    it('keeps replaying even after the order has moved on', async () => {
      const h = createHarness();
      const order = await seedPendingOrder(h);
      const first = await h.service.initiatePayment({ orderId: order.id, idempotencyKey: 'key-1' });

      // The payment completes; a client that never saw the first response retries its request.
      await h.service.onPaymentSucceeded(
        meta(),
        paymentSucceeded(h.uow.getOrder(order.id)!),
      );
      const retry = await h.service.initiatePayment({ orderId: order.id, idempotencyKey: 'key-1' });

      expect(retry.replayed).toBe(true);
      expect(retry.body).toEqual(first.body);
    });

    it('rejects reuse of a key for a different request', async () => {
      const h = createHarness();
      const order = await seedPendingOrder(h);

      await h.service.initiatePayment({ orderId: order.id, idempotencyKey: 'key-1' });

      await expect(
        h.service.initiatePayment({ orderId: order.id, idempotencyKey: 'key-1', currency: 'EUR' }),
      ).rejects.toBeInstanceOf(IdempotencyConflictError);
    });

    it('rejects reuse of a key for a different order', async () => {
      const h = createHarness();
      const a = await seedPendingOrder(h);
      const b = await seedPendingOrder(h);

      await h.service.initiatePayment({ orderId: a.id, idempotencyKey: 'shared-key' });

      await expect(
        h.service.initiatePayment({ orderId: b.id, idempotencyKey: 'shared-key' }),
      ).rejects.toBeInstanceOf(IdempotencyConflictError);
      expect(h.uow.getOrder(b.id)!.paymentId).toBeNull();
    });

    it('does not store failed attempts, so the key is reusable after the problem is fixed', async () => {
      const h = createHarness();
      const missing = createId();

      await expect(
        h.service.initiatePayment({ orderId: missing, idempotencyKey: 'key-1' }),
      ).rejects.toBeInstanceOf(NotFoundError);
      expect(h.uow.idempotencyCount).toBe(0);

      const order = await seedPendingOrder(h);
      const ok = await h.service.initiatePayment({ orderId: order.id, idempotencyKey: 'key-1' });
      expect(ok.replayed).toBe(false);
    });
  });

  describe('guards', () => {
    it('404s for an unknown order', async () => {
      const h = createHarness();

      await expect(
        h.service.initiatePayment({ orderId: createId(), idempotencyKey: 'k' }),
      ).rejects.toBeInstanceOf(NotFoundError);
    });

    it('refuses a second payment under a different key while one is in flight', async () => {
      const h = createHarness();
      const order = await seedOrderWithPayment(h);
      const outboxBefore = h.uow.outbox.length;

      await expect(
        h.service.initiatePayment({ orderId: order.id, idempotencyKey: 'another-key' }),
      ).rejects.toBeInstanceOf(PaymentAlreadyInitiatedError);

      expect(h.uow.outbox).toHaveLength(outboxBefore);
      expect(h.uow.getOrder(order.id)!.paymentId).toBe(order.paymentId);
    });

    it.each(['CONFIRMED', 'FAILED', 'EXPIRED'] as const)(
      'refuses to pay a %s order and leaves no side effects',
      async (status) => {
        const h = createHarness();
        const order = await seedPendingOrder(h);
        h.uow.forceStatus(order.id, status);

        await expect(
          h.service.initiatePayment({ orderId: order.id, idempotencyKey: 'k' }),
        ).rejects.toBeInstanceOf(OrderNotPayableError);

        expect(h.uow.outboxOfType('PaymentRequested')).toHaveLength(0);
        expect(h.uow.getOrder(order.id)!.paymentId).toBeNull();
        expect(h.uow.idempotencyCount).toBe(0);
      },
    );

    it('refuses to pay an order whose hold expired', async () => {
      const h = createHarness();
      const order = await seedPendingOrder(h);
      await h.service.onHoldExpired(meta(), holdExpired(order));

      await expect(
        h.service.initiatePayment({ orderId: order.id, idempotencyKey: 'k' }),
      ).rejects.toBeInstanceOf(OrderNotPayableError);
    });
  });
});
