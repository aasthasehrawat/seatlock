import { afterEach, describe, expect, it } from 'vitest';
import { EVENT_TYPES, createId } from '@seatlock/shared';
import { buildApp } from '../app.js';
import { createLogger } from '../lib/logger.js';
import { InMemoryPaymentRepository } from '../test-support/in-memory-repository.js';

const logger = createLogger({ level: 'silent', name: 'test' });

let app: Awaited<ReturnType<typeof buildApp>> | undefined;

async function setup(options: { enableSimulateRoute?: boolean; ready?: boolean } = {}) {
  const repo = new InMemoryPaymentRepository();
  app = await buildApp({
    logger,
    repo,
    producerName: 'payment-service',
    enableSimulateRoute: options.enableSimulateRoute ?? true,
    readinessChecks: { database: async () => options.ready ?? true },
  });
  return { app, repo };
}

async function seedPending(repo: InMemoryPaymentRepository) {
  const request = {
    id: createId(),
    orderId: createId(),
    holdId: createId(),
    userId: createId(),
    amountCents: 5000,
    currency: 'USD',
  };
  await repo.withTransaction((tx) => tx.insertPendingIfAbsent(request));
  return request;
}

afterEach(async () => {
  await app?.close();
  app = undefined;
});

describe('health routes', () => {
  it('reports live', async () => {
    const { app } = await setup();
    const res = await app.inject({ method: 'GET', url: '/health/live' });
    expect(res.statusCode).toBe(200);
    expect(res.json()).toEqual({ status: 'ok' });
  });

  it('reports ready and not ready', async () => {
    const ready = await setup();
    expect((await ready.app.inject({ method: 'GET', url: '/health/ready' })).statusCode).toBe(200);
    await app?.close();

    const notReady = await setup({ ready: false });
    const res = await notReady.app.inject({ method: 'GET', url: '/health/ready' });
    expect(res.statusCode).toBe(503);
    expect(res.json()).toMatchObject({ status: 'not_ready', checks: { database: false } });
  });
});

describe('payment routes', () => {
  it('gets a payment by id', async () => {
    const { app, repo } = await setup();
    const seeded = await seedPending(repo);

    const res = await app.inject({ method: 'GET', url: `/payments/${seeded.id}` });
    expect(res.statusCode).toBe(200);
    expect(res.json()).toMatchObject({ id: seeded.id, orderId: seeded.orderId, status: 'PENDING', amountCents: 5000 });
  });

  it('returns 404 for an unknown payment and 400 for a malformed id', async () => {
    const { app } = await setup();
    const missing = await app.inject({ method: 'GET', url: `/payments/${createId()}` });
    expect(missing.statusCode).toBe(404);
    expect(missing.json()).toMatchObject({ error: 'NOT_FOUND' });

    const bad = await app.inject({ method: 'GET', url: '/payments/not-a-uuid' });
    expect(bad.statusCode).toBe(400);
    expect(bad.json()).toMatchObject({ error: 'VALIDATION_ERROR' });
  });

  it('lists payments filtered by orderId', async () => {
    const { app, repo } = await setup();
    const a = await seedPending(repo);
    await seedPending(repo);

    const filtered = await app.inject({ method: 'GET', url: `/payments?orderId=${a.orderId}` });
    expect(filtered.statusCode).toBe(200);
    expect(filtered.json().payments).toHaveLength(1);
    expect(filtered.json().payments[0].id).toBe(a.id);

    const all = await app.inject({ method: 'GET', url: '/payments' });
    expect(all.json().payments).toHaveLength(2);
  });

  it('simulates success and publishes an event', async () => {
    const { app, repo } = await setup();
    const seeded = await seedPending(repo);

    const res = await app.inject({
      method: 'POST',
      url: `/payments/${seeded.id}/simulate`,
      payload: { outcome: 'succeed' },
    });
    expect(res.statusCode).toBe(200);
    expect(res.json()).toMatchObject({ status: 'SUCCEEDED' });
    expect(repo.outboxOfType(EVENT_TYPES.PaymentSucceeded)).toHaveLength(1);
  });

  it('simulates failure and rejects a second simulate on a settled payment', async () => {
    const { app, repo } = await setup();
    const seeded = await seedPending(repo);

    const fail = await app.inject({
      method: 'POST',
      url: `/payments/${seeded.id}/simulate`,
      payload: { outcome: 'fail', reason: 'CARD_DECLINED' },
    });
    expect(fail.statusCode).toBe(200);
    expect(fail.json()).toMatchObject({ status: 'FAILED', failureReason: 'CARD_DECLINED' });

    const again = await app.inject({
      method: 'POST',
      url: `/payments/${seeded.id}/simulate`,
      payload: { outcome: 'succeed' },
    });
    expect(again.statusCode).toBe(409);
    expect(again.json()).toMatchObject({ error: 'INVALID_STATE_TRANSITION' });
  });

  it('validates the simulate body', async () => {
    const { app, repo } = await setup();
    const seeded = await seedPending(repo);
    const res = await app.inject({
      method: 'POST',
      url: `/payments/${seeded.id}/simulate`,
      payload: { outcome: 'maybe' },
    });
    expect(res.statusCode).toBe(400);
  });

  it('does not expose simulate when disabled', async () => {
    const { app, repo } = await setup({ enableSimulateRoute: false });
    const seeded = await seedPending(repo);
    const res = await app.inject({
      method: 'POST',
      url: `/payments/${seeded.id}/simulate`,
      payload: { outcome: 'succeed' },
    });
    expect(res.statusCode).toBe(404);
  });
});
