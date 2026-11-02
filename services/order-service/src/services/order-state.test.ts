import { describe, expect, it } from 'vitest';
import {
  NotFoundError,
  ORDER_STATES,
  assertOrderTransition,
  createId,
  type OrderState,
} from '@seatlock/shared';
import { NonRetryableError } from '../lib/errors.js';
import {
  createHarness,
  holdExpired,
  inventoryReleased,
  inventoryReserved,
  meta,
  paymentFailed,
  paymentSucceeded,
  seedOrderWithPayment,
  seedPendingOrder,
} from '../testing/fixtures.js';

describe('shared order state machine (contract this service relies on)', () => {
  const states = Object.values(ORDER_STATES);
  const legal: Array<[OrderState, OrderState]> = [
    ['PENDING_PAYMENT', 'CONFIRMED'],
    ['PENDING_PAYMENT', 'FAILED'],
    ['PENDING_PAYMENT', 'EXPIRED'],
  ];

  it.each(legal)('allows %s -> %s', (from, to) => {
    expect(() => assertOrderTransition(from, to)).not.toThrow();
  });

  it('rejects every other transition, including any move out of a terminal state', () => {
    for (const from of states) {
      for (const to of states) {
        if (legal.some(([f, t]) => f === from && t === to)) continue;
        expect(() => assertOrderTransition(from, to), `${from} -> ${to}`).toThrow();
      }
    }
  });
});

describe('InventoryReserved -> order creation', () => {
  it('creates a PENDING_PAYMENT order and an OrderCreated outbox row in one step', async () => {
    const h = createHarness();
    const payload = inventoryReserved({ quantity: 3, unitPriceCents: 2500 });

    const result = await h.service.onInventoryReserved(meta({ correlationId: 'corr-42' }), payload);

    expect(result.outcome).toBe('applied');
    const order = h.uow.getOrder(result.orderId!)!;
    expect(order).toMatchObject({
      holdId: payload.holdId,
      eventId: payload.eventId,
      userId: payload.userId,
      quantity: 3,
      amountCents: 7500, // integer cents: quantity * unit price
      status: 'PENDING_PAYMENT',
      paymentId: null,
    });

    const [row, ...rest] = h.uow.outbox;
    expect(rest).toHaveLength(0);
    expect(row).toMatchObject({
      aggregateType: 'order',
      aggregateId: order.id,
      eventType: 'OrderCreated',
      published: false,
    });
    expect(row!.payload).toEqual({
      orderId: order.id,
      holdId: payload.holdId,
      eventId: payload.eventId,
      userId: payload.userId,
      quantity: 3,
      amountCents: 7500,
    });
    // Same correlation id end to end, and orderId as the Kafka partition key.
    expect(row!.headers).toMatchObject({ correlationId: 'corr-42', partitionKey: order.id });
  });

  it('is idempotent for a redelivered message (same eventId)', async () => {
    const h = createHarness();
    const m = meta();
    const payload = inventoryReserved();

    const first = await h.service.onInventoryReserved(m, payload);
    const second = await h.service.onInventoryReserved(m, payload);

    expect(first.outcome).toBe('applied');
    expect(second.outcome).toBe('duplicate');
    expect(h.uow.orders).toHaveLength(1);
    expect(h.uow.outbox).toHaveLength(1);
  });

  it('does not create a second order when the same hold arrives under a new eventId', async () => {
    const h = createHarness();
    const payload = inventoryReserved();

    await h.service.onInventoryReserved(meta(), payload);
    const again = await h.service.onInventoryReserved(meta(), payload);

    expect(again).toMatchObject({ outcome: 'ignored', reason: 'order_already_exists_for_hold' });
    expect(h.uow.orders).toHaveLength(1);
    expect(h.uow.outboxOfType('OrderCreated')).toHaveLength(1);
  });

  it('rejects an amount that cannot be stored and leaves no trace (so it can go to the DLQ)', async () => {
    const h = createHarness();

    await expect(
      h.service.onInventoryReserved(
        meta(),
        inventoryReserved({ quantity: 1000, unitPriceCents: 10_000_000 }),
      ),
    ).rejects.toBeInstanceOf(NonRetryableError);

    expect(h.uow.orders).toHaveLength(0);
    expect(h.uow.outbox).toHaveLength(0);
    expect(h.uow.processedCount).toBe(0);
  });
});

describe('PaymentSucceeded -> CONFIRMED', () => {
  it('confirms the order and writes OrderConfirmed', async () => {
    const h = createHarness();
    const order = await seedOrderWithPayment(h);

    const result = await h.service.onPaymentSucceeded(meta(), paymentSucceeded(order));

    expect(result).toMatchObject({ outcome: 'applied', orderId: order.id });
    expect(h.uow.getOrder(order.id)!.status).toBe('CONFIRMED');

    const [row] = h.uow.outboxOfType('OrderConfirmed');
    expect(row!.payload).toEqual({
      orderId: order.id,
      holdId: order.holdId,
      eventId: order.eventId,
      userId: order.userId,
      quantity: order.quantity,
      amountCents: order.amountCents,
      paymentId: order.paymentId,
    });
  });

  it('is idempotent: a second delivery with a new eventId is ignored', async () => {
    const h = createHarness();
    const order = await seedOrderWithPayment(h);

    await h.service.onPaymentSucceeded(meta(), paymentSucceeded(order));
    const again = await h.service.onPaymentSucceeded(meta(), paymentSucceeded(order));

    expect(again).toMatchObject({ outcome: 'ignored', reason: 'already_confirmed' });
    expect(h.uow.outboxOfType('OrderConfirmed')).toHaveLength(1);
  });

  it('ignores a result for a payment id that does not belong to the order', async () => {
    const h = createHarness();
    const order = await seedOrderWithPayment(h);

    const result = await h.service.onPaymentSucceeded(
      meta(),
      paymentSucceeded(order, { paymentId: createId() }),
    );

    expect(result).toMatchObject({ outcome: 'ignored', reason: 'payment_id_mismatch' });
    expect(h.uow.getOrder(order.id)!.status).toBe('PENDING_PAYMENT');
  });

  it('treats an amount mismatch as non-retryable and rolls everything back', async () => {
    const h = createHarness();
    const order = await seedOrderWithPayment(h);
    const processedBefore = h.uow.processedCount;

    await expect(
      h.service.onPaymentSucceeded(meta(), paymentSucceeded(order, { amountCents: 1 })),
    ).rejects.toBeInstanceOf(NonRetryableError);

    expect(h.uow.getOrder(order.id)!.status).toBe('PENDING_PAYMENT');
    expect(h.uow.outboxOfType('OrderConfirmed')).toHaveLength(0);
    expect(h.uow.processedCount).toBe(processedBefore); // dedupe claim rolled back too
  });

  it('throws a retryable error for an unknown order and does not consume the event', async () => {
    const h = createHarness();
    const order = await seedOrderWithPayment(h);
    const m = meta();

    await expect(
      h.service.onPaymentSucceeded(m, paymentSucceeded(order, { orderId: createId() })),
    ).rejects.toBeInstanceOf(NotFoundError);

    // Because the claim rolled back, the very same message can be retried and handled later.
    const retried = await h.service.onPaymentSucceeded(m, paymentSucceeded(order));
    expect(retried.outcome).toBe('applied');
  });
});

describe('PaymentFailed -> FAILED', () => {
  it('fails the order and writes OrderFailed with the reason', async () => {
    const h = createHarness();
    const order = await seedOrderWithPayment(h);

    const result = await h.service.onPaymentFailed(
      meta(),
      paymentFailed(order, { reason: 'insufficient_funds' }),
    );

    expect(result.outcome).toBe('applied');
    expect(h.uow.getOrder(order.id)!.status).toBe('FAILED');
    expect(h.uow.outboxOfType('OrderFailed')[0]!.payload).toEqual({
      orderId: order.id,
      holdId: order.holdId,
      eventId: order.eventId,
      reason: 'insufficient_funds',
    });
  });
});

describe('hold expiry -> EXPIRED', () => {
  it('expires a pending order on InventoryReleased(EXPIRED)', async () => {
    const h = createHarness();
    const order = await seedPendingOrder(h);

    const result = await h.service.onInventoryReleased(meta(), inventoryReleased(order, 'EXPIRED'));

    expect(result.outcome).toBe('applied');
    expect(h.uow.getOrder(order.id)!.status).toBe('EXPIRED');
    expect(h.uow.outboxOfType('OrderExpired')).toHaveLength(1);
  });

  it('expires a pending order on HoldExpired', async () => {
    const h = createHarness();
    const order = await seedPendingOrder(h);

    await h.service.onHoldExpired(meta(), holdExpired(order));

    expect(h.uow.getOrder(order.id)!.status).toBe('EXPIRED');
  });

  it.each(['PAYMENT_FAILED', 'RELEASED', 'CANCELLED'] as const)(
    'ignores InventoryReleased(%s): not an expiry',
    async (reason) => {
      const h = createHarness();
      const order = await seedPendingOrder(h);

      const result = await h.service.onInventoryReleased(meta(), inventoryReleased(order, reason));

      expect(result.outcome).toBe('ignored');
      expect(h.uow.getOrder(order.id)!.status).toBe('PENDING_PAYMENT');
    },
  );

  it('ignores expiry for a hold that never became an order', async () => {
    const h = createHarness();
    const order = await seedPendingOrder(h);

    const result = await h.service.onHoldExpired(meta(), { ...holdExpired(order), holdId: createId() });

    expect(result).toMatchObject({ outcome: 'ignored', reason: 'no_order_for_hold' });
  });
});

describe('terminal states are final', () => {
  const terminal = ['CONFIRMED', 'FAILED', 'EXPIRED'] as const;

  it.each(terminal)('a %s order never moves again, whatever event arrives', async (status) => {
    const h = createHarness();
    const order = await seedOrderWithPayment(h);
    h.uow.forceStatus(order.id, status);
    const outboxBefore = h.uow.outbox.length;

    const results = [
      await h.service.onPaymentSucceeded(meta(), paymentSucceeded(order)),
      await h.service.onPaymentFailed(meta(), paymentFailed(order)),
      await h.service.onHoldExpired(meta(), holdExpired(order)),
      await h.service.onInventoryReleased(meta(), inventoryReleased(order, 'EXPIRED')),
    ];

    for (const r of results) expect(r.outcome).toBe('ignored');
    expect(h.uow.getOrder(order.id)!.status).toBe(status);
    expect(h.uow.outbox).toHaveLength(outboxBefore); // no events emitted for ignored input
  });

  it('flags a payment that succeeds after the order expired (refund needed)', async () => {
    const h = createHarness();
    const order = await seedOrderWithPayment(h);
    await h.service.onHoldExpired(meta(), holdExpired(order));

    const late = await h.service.onPaymentSucceeded(meta(), paymentSucceeded(order));

    expect(late).toMatchObject({ outcome: 'ignored', reason: 'late_payment_for_expired_order' });
    expect(h.uow.getOrder(order.id)!.status).toBe('EXPIRED');
    expect(h.uow.outboxOfType('OrderConfirmed')).toHaveLength(0);
  });
});
