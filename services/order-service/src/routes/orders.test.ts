import { afterEach, beforeEach, describe, expect, it } from 'vitest';
import { createId } from '@seatlock/shared';
import { buildApp } from '../app.js';
import { createHarness, seedPendingOrder, silentLogger, type Harness } from '../testing/fixtures.js';

describe('order HTTP API', () => {
  let h: Harness;
  let app: Awaited<ReturnType<typeof buildApp>>;
  let dbUp = true;

  beforeEach(async () => {
    h = createHarness();
    dbUp = true;
    app = await buildApp({
      logger: silentLogger,
      service: h.service,
      readinessChecks: [{ name: 'postgres', check: async () => dbUp }],
    });
    await app.ready();
  });

  afterEach(async () => {
    await app.close();
  });

  describe('health', () => {
    it('live is always ok', async () => {
      dbUp = false;
      const res = await app.inject({ method: 'GET', url: '/health/live' });
      expect(res.statusCode).toBe(200);
      expect(res.json()).toEqual({ status: 'ok' });
    });

    it('ready reflects dependency state', async () => {
      const up = await app.inject({ method: 'GET', url: '/health/ready' });
      expect(up.statusCode).toBe(200);
      expect(up.json()).toMatchObject({ status: 'ready', checks: { postgres: 'up' } });

      dbUp = false;
      const down = await app.inject({ method: 'GET', url: '/health/ready' });
      expect(down.statusCode).toBe(503);
      expect(down.json()).toMatchObject({ status: 'unavailable', checks: { postgres: 'down' } });
    });
  });

  describe('GET /orders/:id', () => {
    it('returns the order', async () => {
      const order = await seedPendingOrder(h);
      const res = await app.inject({ method: 'GET', url: `/orders/${order.id}` });

      expect(res.statusCode).toBe(200);
      expect(res.json()).toMatchObject({
        id: order.id,
        status: 'PENDING_PAYMENT',
        amountCents: order.amountCents,
        paymentId: null,
      });
    });

    it('404s with the uniform error shape', async () => {
      const res = await app.inject({ method: 'GET', url: `/orders/${createId()}` });
      expect(res.statusCode).toBe(404);
      expect(res.json().error.code).toBe('NOT_FOUND');
    });

    it('400s on a malformed id', async () => {
      const res = await app.inject({ method: 'GET', url: '/orders/not-a-uuid' });
      expect(res.statusCode).toBe(400);
      expect(res.json().error.code).toBe('VALIDATION_ERROR');
    });
  });

  describe('GET /orders?userId=', () => {
    it("lists only that user's orders", async () => {
      const userId = createId();
      await seedPendingOrder(h, { userId });
      await seedPendingOrder(h, { userId });
      await seedPendingOrder(h); // someone else

      const res = await app.inject({ method: 'GET', url: `/orders?userId=${userId}` });

      expect(res.statusCode).toBe(200);
      const body = res.json();
      expect(body.items).toHaveLength(2);
      expect(body.items.every((o: { userId: string }) => o.userId === userId)).toBe(true);
    });

    it('requires userId', async () => {
      const res = await app.inject({ method: 'GET', url: '/orders' });
      expect(res.statusCode).toBe(400);
    });
  });

  describe('POST /orders/:id/pay', () => {
    it('requires the Idempotency-Key header', async () => {
      const order = await seedPendingOrder(h);
      const res = await app.inject({ method: 'POST', url: `/orders/${order.id}/pay` });

      expect(res.statusCode).toBe(400);
      expect(res.json().error.code).toBe('VALIDATION_ERROR');
      expect(h.uow.outboxOfType('PaymentRequested')).toHaveLength(0);
    });

    it('returns 202 with payment details', async () => {
      const order = await seedPendingOrder(h);
      const res = await app.inject({
        method: 'POST',
        url: `/orders/${order.id}/pay`,
        headers: { 'idempotency-key': 'abc-123' },
      });

      expect(res.statusCode).toBe(202);
      expect(res.headers['idempotent-replayed']).toBeUndefined();
      expect(res.json()).toMatchObject({
        orderId: order.id,
        paymentStatus: 'REQUESTED',
        amountCents: order.amountCents,
        currency: 'USD',
      });
    });

    it('replays an identical retry and marks it as replayed', async () => {
      const order = await seedPendingOrder(h);
      const request = {
        method: 'POST' as const,
        url: `/orders/${order.id}/pay`,
        headers: { 'idempotency-key': 'abc-123' },
      };

      const first = await app.inject(request);
      const second = await app.inject(request);

      expect(second.statusCode).toBe(202);
      expect(second.headers['idempotent-replayed']).toBe('true');
      expect(second.json()).toEqual(first.json());
      expect(h.uow.outboxOfType('PaymentRequested')).toHaveLength(1);
    });

    it('409s when a key is reused with a different request body', async () => {
      const order = await seedPendingOrder(h);
      const url = `/orders/${order.id}/pay`;
      const headers = { 'idempotency-key': 'abc-123' };

      await app.inject({ method: 'POST', url, headers });
      const res = await app.inject({ method: 'POST', url, headers, payload: { currency: 'EUR' } });

      expect(res.statusCode).toBe(409);
      expect(res.json().error.code).toBe('IDEMPOTENCY_CONFLICT');
    });

    it('400s on an invalid currency', async () => {
      const order = await seedPendingOrder(h);
      const res = await app.inject({
        method: 'POST',
        url: `/orders/${order.id}/pay`,
        headers: { 'idempotency-key': 'k' },
        payload: { currency: 'DOLLARS' },
      });
      expect(res.statusCode).toBe(400);
    });

    it('404s for an unknown order', async () => {
      const res = await app.inject({
        method: 'POST',
        url: `/orders/${createId()}/pay`,
        headers: { 'idempotency-key': 'k' },
      });
      expect(res.statusCode).toBe(404);
    });

    it('409s when the order is no longer payable', async () => {
      const order = await seedPendingOrder(h);
      h.uow.forceStatus(order.id, 'EXPIRED');

      const res = await app.inject({
        method: 'POST',
        url: `/orders/${order.id}/pay`,
        headers: { 'idempotency-key': 'k' },
      });

      expect(res.statusCode).toBe(409);
      expect(res.json().error.code).toBe('ORDER_NOT_PAYABLE');
    });
  });

  it('returns a JSON 404 for unknown routes', async () => {
    const res = await app.inject({ method: 'GET', url: '/nope' });
    expect(res.statusCode).toBe(404);
    expect(res.json().error.code).toBe('ROUTE_NOT_FOUND');
  });
});
