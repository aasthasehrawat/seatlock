import { execSync } from 'node:child_process';
import type { Pool } from 'pg';
import type { StartedPostgreSqlContainer } from '@testcontainers/postgresql';
import { afterAll, beforeAll, beforeEach, describe, expect, it, vi } from 'vitest';
import { createId, TOPICS } from '@seatlock/shared';
import { runMigrations } from '../../src/db/migrate.js';
import { createPool } from '../../src/db/pool.js';
import { PgUnitOfWork } from '../../src/db/unit-of-work.js';
import { OrderService } from '../../src/services/order-service.js';
import { OutboxRelay } from '../../src/workers/outbox-relay.js';
import { silentLogger } from '../../src/testing/fixtures.js';

/**
 * Integration tests against a REAL Postgres started by Testcontainers.
 *
 * They exist for the things the in-memory fake cannot prove: row locks, advisory locks, unique
 * constraints, real transaction rollback, and the actual SQL. Skipped automatically when Docker
 * is not available, so `npm test` stays green on machines without it.
 *
 * Run with:  npm run test:integration
 */

function dockerAvailable(): boolean {
  try {
    execSync('docker info', { stdio: 'ignore', timeout: 10_000 });
    return true;
  } catch {
    return false;
  }
}

const hasDocker = dockerAvailable();

describe.skipIf(!hasDocker)('order-service integration (Postgres via Testcontainers)', () => {
  let container: StartedPostgreSqlContainer;
  let pool: Pool;
  let uow: PgUnitOfWork;
  let service: OrderService;

  beforeAll(async () => {
    // Imported lazily so merely collecting this file never needs the container libraries.
    const { PostgreSqlContainer } = await import('@testcontainers/postgresql');
    container = await new PostgreSqlContainer('postgres:16-alpine').start();

    pool = createPool({ databaseUrl: container.getConnectionUri(), dbPoolMax: 10 }, silentLogger);
    await runMigrations(pool, silentLogger);

    uow = new PgUnitOfWork(pool);
    service = new OrderService({ uow, logger: silentLogger });
  });

  afterAll(async () => {
    await pool?.end();
    await container?.stop();
  });

  beforeEach(async () => {
    await pool.query('TRUNCATE orders, idempotency_keys, outbox, processed_events');
  });

  const meta = () => ({ eventId: createId(), correlationId: 'corr-it' });
  const reserved = () => ({
    holdId: createId(),
    eventId: createId(),
    userId: createId(),
    quantity: 2,
    unitPriceCents: 4500,
    expiresAt: new Date(Date.now() + 300_000).toISOString(),
    reservationId: createId(),
  });

  async function createOrder() {
    const result = await service.onInventoryReserved(meta(), reserved());
    return service.getOrder(result.orderId!);
  }

  describe('migrations', () => {
    it('are idempotent', async () => {
      expect(await runMigrations(pool, silentLogger)).toEqual([]);
    });
  });

  describe('InventoryReserved consumer', () => {
    it('writes the order, the outbox row and the dedupe claim atomically', async () => {
      const m = meta();
      const payload = reserved();

      const result = await service.onInventoryReserved(m, payload);

      expect(result.outcome).toBe('applied');
      const orders = await pool.query('SELECT status, amount_cents FROM orders WHERE hold_id = $1', [
        payload.holdId,
      ]);
      expect(orders.rows).toEqual([{ status: 'PENDING_PAYMENT', amount_cents: 9000 }]);

      const outbox = await pool.query('SELECT event_type, published FROM outbox');
      expect(outbox.rows).toEqual([{ event_type: 'OrderCreated', published: false }]);

      const processed = await pool.query('SELECT 1 FROM processed_events WHERE event_id = $1', [
        m.eventId,
      ]);
      expect(processed.rowCount).toBe(1);
    });

    it('survives concurrent redelivery of the same message', async () => {
      const m = meta();
      const payload = reserved();

      const results = await Promise.all(
        Array.from({ length: 5 }, () => service.onInventoryReserved(m, payload)),
      );

      expect(results.filter((r) => r.outcome === 'applied')).toHaveLength(1);
      expect((await pool.query('SELECT 1 FROM orders')).rowCount).toBe(1);
      expect((await pool.query('SELECT 1 FROM outbox')).rowCount).toBe(1);
    });

    it('enforces one order per hold even for different message ids', async () => {
      const payload = reserved();

      await Promise.all([
        service.onInventoryReserved(meta(), payload),
        service.onInventoryReserved(meta(), payload),
      ]);

      expect((await pool.query('SELECT 1 FROM orders')).rowCount).toBe(1);
      expect((await pool.query('SELECT 1 FROM outbox')).rowCount).toBe(1);
    });
  });

  describe('POST /orders/:id/pay semantics', () => {
    it('creates exactly one payment when the same key is sent concurrently', async () => {
      const order = await createOrder();

      const results = await Promise.all(
        Array.from({ length: 8 }, () =>
          service.initiatePayment({ orderId: order.id, idempotencyKey: 'concurrent-key' }),
        ),
      );

      expect(new Set(results.map((r) => r.body.paymentId)).size).toBe(1);
      expect(results.filter((r) => !r.replayed)).toHaveLength(1);
      const requested = await pool.query(
        "SELECT 1 FROM outbox WHERE event_type = 'PaymentRequested'",
      );
      expect(requested.rowCount).toBe(1);
    });

    it('lets only one of several different keys start a payment', async () => {
      const order = await createOrder();

      const settled = await Promise.allSettled(
        ['a', 'b', 'c'].map((k) => service.initiatePayment({ orderId: order.id, idempotencyKey: k })),
      );

      expect(settled.filter((s) => s.status === 'fulfilled')).toHaveLength(1);
      expect(
        (await pool.query("SELECT 1 FROM outbox WHERE event_type = 'PaymentRequested'")).rowCount,
      ).toBe(1);
    });
  });

  describe('payment results', () => {
    it('confirms the order and queues OrderConfirmed', async () => {
      const order = await createOrder();
      const { body } = await service.initiatePayment({ orderId: order.id, idempotencyKey: 'k' });

      await service.onPaymentSucceeded(meta(), {
        paymentId: body.paymentId,
        orderId: order.id,
        holdId: order.holdId,
        amountCents: order.amountCents,
        providerRef: 'prov_1',
      });

      expect((await service.getOrder(order.id)).status).toBe('CONFIRMED');
      const events = await pool.query('SELECT event_type FROM outbox ORDER BY created_at');
      expect(events.rows.map((r) => r.event_type)).toEqual([
        'OrderCreated',
        'PaymentRequested',
        'OrderConfirmed',
      ]);
    });

    it('applies a duplicated PaymentSucceeded exactly once under concurrency', async () => {
      const order = await createOrder();
      const { body } = await service.initiatePayment({ orderId: order.id, idempotencyKey: 'k' });
      const payload = {
        paymentId: body.paymentId,
        orderId: order.id,
        holdId: order.holdId,
        amountCents: order.amountCents,
        providerRef: 'prov_1',
      };

      await Promise.all(Array.from({ length: 4 }, () => service.onPaymentSucceeded(meta(), payload)));

      expect(
        (await pool.query("SELECT 1 FROM outbox WHERE event_type = 'OrderConfirmed'")).rowCount,
      ).toBe(1);
    });

    it('lets exactly one terminal transition win when success and expiry race', async () => {
      const order = await createOrder();
      const { body } = await service.initiatePayment({ orderId: order.id, idempotencyKey: 'k' });

      await Promise.all([
        service.onPaymentSucceeded(meta(), {
          paymentId: body.paymentId,
          orderId: order.id,
          holdId: order.holdId,
          amountCents: order.amountCents,
          providerRef: 'prov_1',
        }),
        service.onHoldExpired(meta(), {
          holdId: order.holdId,
          eventId: order.eventId,
          quantity: order.quantity,
        }),
      ]);

      const final = (await service.getOrder(order.id)).status;
      expect(['CONFIRMED', 'EXPIRED']).toContain(final);
      const terminalEvents = await pool.query(
        "SELECT event_type FROM outbox WHERE event_type IN ('OrderConfirmed', 'OrderExpired')",
      );
      expect(terminalEvents.rowCount).toBe(1);
    });
  });

  describe('outbox relay', () => {
    it('publishes rows once and survives an empty poll', async () => {
      const order = await createOrder();
      await service.initiatePayment({ orderId: order.id, idempotencyKey: 'k' });

      const sendBatch = vi.fn().mockResolvedValue([]);
      const relay = new OutboxRelay({
        uow,
        producer: { sendBatch } as never,
        logger: silentLogger,
        pollIntervalMs: 10,
        batchSize: 100,
        retentionHours: 24,
        idempotencyTtlHours: 24,
      });

      expect(await relay.runOnce()).toBe(2);
      expect(await relay.runOnce()).toBe(0);

      const topics = sendBatch.mock.calls[0]![0].topicMessages.map(
        (t: { topic: string }) => t.topic,
      );
      expect(topics.sort()).toEqual([TOPICS.ORDERS, TOPICS.PAYMENTS].sort());
      expect(
        (await pool.query('SELECT 1 FROM outbox WHERE published = FALSE')).rowCount,
      ).toBe(0);
    });

    it('lets only one relay publish at a time (advisory lock)', async () => {
      const order = await createOrder();
      await service.initiatePayment({ orderId: order.id, idempotencyKey: 'k' });

      let release!: () => void;
      const gate = new Promise<void>((resolve) => (release = resolve));
      const slowSend = vi.fn().mockImplementation(() => gate);
      const fastSend = vi.fn().mockResolvedValue([]);

      const make = (sendBatch: unknown) =>
        new OutboxRelay({
          uow,
          producer: { sendBatch } as never,
          logger: silentLogger,
          pollIntervalMs: 10,
          batchSize: 100,
          retentionHours: 24,
          idempotencyTtlHours: 24,
        });

      const first = make(slowSend).runOnce(); // holds the lock while "sending"
      await vi.waitFor(() => expect(slowSend).toHaveBeenCalled());

      expect(await make(fastSend).runOnce()).toBe(0); // second relay backs off
      expect(fastSend).not.toHaveBeenCalled();

      release();
      expect(await first).toBe(2);
    });
  });

  // The full path (Kafka in, Kafka out, retry topic, DLQ) needs a Kafka container and the payment
  // service's contract. Left as an explicit TODO rather than a flaky half-test.
  it.todo('consumes InventoryReserved from Kafka and publishes OrderCreated to orders.events');
  it.todo('routes a transiently failing message through retry.5s and succeeds on the retry');
  it.todo('dead-letters a poison message to dlq');
});
