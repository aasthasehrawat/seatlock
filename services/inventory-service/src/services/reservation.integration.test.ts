import { IdempotencyConflictError, SoldOutError, createId } from '@seatlock/shared';
import { PostgreSqlContainer, type StartedPostgreSqlContainer } from '@testcontainers/postgresql';
import pg from 'pg';
import { afterAll, beforeAll, describe, expect, it, type TestContext } from 'vitest';
import { runMigrations } from '../db/migrate.js';
import { sweepExpiredHolds } from '../workers/hold-expiry.js';
import type { ServiceDeps } from './deps.js';
import { confirmHold, releaseHold } from './hold-lifecycle.js';
import { reserveTickets } from './reservation.js';
import { joinWaitlist } from './waitlist.js';

/**
 * Integration tests against a REAL PostgreSQL, because the property we care about
 * (no overselling under concurrency) is a property of the database's locking, which a mock
 * cannot demonstrate.
 *
 * Database source, in order of preference:
 *   1. TEST_DATABASE_URL (an existing Postgres you point us at), or
 *   2. a throwaway container started with Testcontainers (requires Docker).
 * If neither is available the whole suite is SKIPPED with a clear message (not failed), so
 * `npm test` stays green on machines without Docker.
 */

let container: StartedPostgreSqlContainer | undefined;
let pool: pg.Pool | undefined;
let skipReason: string | undefined;

beforeAll(async () => {
  try {
    let connectionString = process.env.TEST_DATABASE_URL;
    if (!connectionString) {
      container = await new PostgreSqlContainer('postgres:16-alpine').start();
      connectionString = container.getConnectionUri();
    }
    pool = new pg.Pool({ connectionString, max: 30 });
    await runMigrations(pool);
  } catch (err) {
    skipReason =
      'Skipping integration tests: no PostgreSQL available. Start Docker (Testcontainers) or set ' +
      `TEST_DATABASE_URL. Cause: ${(err as Error).message}`;
    // eslint-disable-next-line no-console
    console.warn(`\n[reservation.integration.test] ${skipReason}\n`);
    await pool?.end().catch(() => undefined);
    pool = undefined;
    await container?.stop().catch(() => undefined);
    container = undefined;
  }
});

afterAll(async () => {
  await pool?.end();
  await container?.stop();
});

/** Returns the pool, or skips the current test (with the reason) when no database is available. */
function db(ctx: TestContext): pg.Pool {
  if (!pool) ctx.skip(skipReason);
  return pool!;
}

function depsFor(p: pg.Pool, holdTtlSeconds = 300): ServiceDeps {
  return { pool: p, holdTtlSeconds, serviceName: 'inventory-service-test' };
}

async function createEvent(p: pg.Pool, total: number): Promise<string> {
  const id = createId();
  await p.query(
    `INSERT INTO events (id, name, starts_at, total_tickets, available, unit_price_cents)
     VALUES ($1, 'Test Concert', NOW() + INTERVAL '30 days', $2, $2, 5000)`,
    [id, total],
  );
  return id;
}

async function available(p: pg.Pool, eventId: string): Promise<number> {
  const { rows } = await p.query<{ available: number }>('SELECT available FROM events WHERE id = $1', [eventId]);
  return rows[0]!.available;
}

async function reserve(deps: ServiceDeps, eventId: string, quantity: number, userId = createId(), key = createId()) {
  return reserveTickets(deps, {
    eventId,
    userId,
    quantity,
    idempotencyKey: key,
    correlationId: createId(),
  });
}

/** Per-concert accounting invariant: available + ACTIVE + CONFIRMED = total. */
async function expectInventoryBalanced(p: pg.Pool, eventId: string): Promise<void> {
  const { rows } = await p.query<{ total: number; avail: number; held: string }>(
    `SELECT e.total_tickets AS total, e.available AS avail,
            COALESCE((SELECT SUM(quantity) FROM holds h
                       WHERE h.event_id = e.id AND h.status IN ('ACTIVE','CONFIRMED')), 0)::text AS held
       FROM events e WHERE e.id = $1`,
    [eventId],
  );
  const { total, avail, held } = rows[0]!;
  expect(avail).toBeGreaterThanOrEqual(0);
  expect(avail + Number(held)).toBe(total);
}

describe('reservation (integration, real PostgreSQL)', () => {
  it('never oversells: 300 concurrent single-ticket requests for 100 tickets -> exactly 100 succeed', async (ctx) => {
    const p = db(ctx);
    const deps = depsFor(p);
    const eventId = await createEvent(p, 100);

    const results = await Promise.allSettled(Array.from({ length: 300 }, () => reserve(deps, eventId, 1)));

    const ok = results.filter((r) => r.status === 'fulfilled');
    const failed = results.filter((r): r is PromiseRejectedResult => r.status === 'rejected');

    expect(ok).toHaveLength(100);
    expect(failed).toHaveLength(200);
    // Every failure must be a clean SoldOut, never a deadlock / serialization / constraint error.
    for (const f of failed) expect(f.reason).toBeInstanceOf(SoldOutError);

    expect(await available(p, eventId)).toBe(0);
    const holds = await p.query(`SELECT 1 FROM holds WHERE event_id = $1 AND status = 'ACTIVE'`, [eventId]);
    expect(holds.rowCount).toBe(100);

    // Transactional outbox: exactly one InventoryReserved per successful reservation, none for failures.
    const outbox = await p.query(
      `SELECT 1 FROM outbox WHERE event_type = 'InventoryReserved' AND headers->>'eventId' = $1 AND published = FALSE`,
      [eventId],
    );
    expect(outbox.rowCount).toBe(100);
    await expectInventoryBalanced(p, eventId);
  });

  it('never oversells with mixed quantities (1-4 tickets per request)', async (ctx) => {
    const p = db(ctx);
    const deps = depsFor(p);
    const total = 50;
    const eventId = await createEvent(p, total);

    const quantities = Array.from({ length: 80 }, (_, i) => (i % 4) + 1);
    const results = await Promise.allSettled(quantities.map((q) => reserve(deps, eventId, q)));

    const granted = results.reduce((sum, r) => (r.status === 'fulfilled' ? sum + r.value.body.quantity : sum), 0);
    expect(granted).toBeLessThanOrEqual(total);
    expect(await available(p, eventId)).toBe(total - granted);
    for (const r of results) {
      if (r.status === 'rejected') expect(r.reason).toBeInstanceOf(SoldOutError);
    }
    await expectInventoryBalanced(p, eventId);
  });

  it('concurrent retries with the SAME Idempotency-Key create exactly one hold', async (ctx) => {
    const p = db(ctx);
    const deps = depsFor(p);
    const eventId = await createEvent(p, 10);
    const userId = createId();
    const key = createId();

    const results = await Promise.all(Array.from({ length: 10 }, () => reserve(deps, eventId, 2, userId, key)));

    expect(new Set(results.map((r) => r.body.holdId)).size).toBe(1);
    expect(results.filter((r) => !r.replayed)).toHaveLength(1);
    expect(await available(p, eventId)).toBe(8);
    const holds = await p.query('SELECT 1 FROM holds WHERE event_id = $1', [eventId]);
    expect(holds.rowCount).toBe(1);
  });

  it('rejects an Idempotency-Key reused with a different body and leaves stock untouched', async (ctx) => {
    const p = db(ctx);
    const deps = depsFor(p);
    const eventId = await createEvent(p, 10);
    const userId = createId();
    const key = createId();

    await reserve(deps, eventId, 2, userId, key);
    await expect(reserve(deps, eventId, 5, userId, key)).rejects.toBeInstanceOf(IdempotencyConflictError);
    expect(await available(p, eventId)).toBe(8);
  });

  it('a sold-out attempt leaves no hold, no outbox row and no idempotency record', async (ctx) => {
    const p = db(ctx);
    const deps = depsFor(p);
    const eventId = await createEvent(p, 1);
    await reserve(deps, eventId, 1);

    const key = createId();
    await expect(reserve(deps, eventId, 1, createId(), key)).rejects.toBeInstanceOf(SoldOutError);

    const rec = await p.query('SELECT 1 FROM idempotency_keys WHERE key = $1', [`reservation:${key}`]);
    expect(rec.rowCount).toBe(0);
    const holds = await p.query('SELECT 1 FROM holds WHERE event_id = $1', [eventId]);
    expect(holds.rowCount).toBe(1);
  });
});

describe('hold lifecycle (integration, real PostgreSQL)', () => {
  it('concurrent releases of one hold restore stock exactly once', async (ctx) => {
    const p = db(ctx);
    const deps = depsFor(p);
    const eventId = await createEvent(p, 10);
    const { body } = await reserve(deps, eventId, 4);
    expect(await available(p, eventId)).toBe(6);

    const results = await Promise.all(
      Array.from({ length: 10 }, (_, i) => releaseHold(deps, body.holdId, i % 2 ? 'EXPIRED' : 'PAYMENT_FAILED')),
    );

    expect(results.filter((r) => r.released)).toHaveLength(1);
    expect(await available(p, eventId)).toBe(10); // not 10 + 9*4
    await expectInventoryBalanced(p, eventId);
  });

  it('a CONFIRMED hold is never released (stock stays deducted)', async (ctx) => {
    const p = db(ctx);
    const deps = depsFor(p);
    const eventId = await createEvent(p, 10);
    const { body } = await reserve(deps, eventId, 3);

    await confirmHold(deps, body.holdId, createId());
    const result = await releaseHold(deps, body.holdId, 'EXPIRED');

    expect(result.released).toBe(false);
    expect(result.currentStatus).toBe('CONFIRMED');
    expect(await available(p, eventId)).toBe(7);
    await expectInventoryBalanced(p, eventId);
  });

  it('confirm-vs-expire race has exactly one winner and inventory always balances', async (ctx) => {
    const p = db(ctx);
    const deps = depsFor(p);
    const eventId = await createEvent(p, 40);

    for (let round = 0; round < 20; round++) {
      const { body } = await reserve(deps, eventId, 2);
      const orderId = createId();
      const [confirm, release] = await Promise.allSettled([
        confirmHold(deps, body.holdId, orderId),
        releaseHold(deps, body.holdId, 'EXPIRED'),
      ]);

      const { rows } = await p.query<{ status: string }>('SELECT status FROM holds WHERE id = $1', [body.holdId]);
      const status = rows[0]!.status;
      expect(['CONFIRMED', 'EXPIRED']).toContain(status);
      if (status === 'CONFIRMED') {
        expect(confirm.status).toBe('fulfilled');
        expect(release.status === 'fulfilled' && release.value.released).toBe(false);
      } else {
        expect(release.status === 'fulfilled' && release.value.released).toBe(true);
        expect(confirm.status).toBe('rejected'); // InvalidStateTransitionError
      }
    }
    await expectInventoryBalanced(p, eventId);
  });

  it('expiry sweeper releases overdue ACTIVE holds and leaves live ones alone', async (ctx) => {
    const p = db(ctx);
    const deps = depsFor(p);
    const eventId = await createEvent(p, 10);
    const overdue = (await reserve(deps, eventId, 2)).body.holdId;
    const live = (await reserve(deps, eventId, 3)).body.holdId;
    await p.query(`UPDATE holds SET expires_at = NOW() - INTERVAL '1 minute' WHERE id = $1`, [overdue]);

    const logger = { info() {}, error() {}, warn() {}, debug() {} } as never;
    const sweep = await sweepExpiredHolds({ deps, pool: p, batchSize: 100, logger });
    // Sweeping again must be a no-op (idempotent).
    const again = await sweepExpiredHolds({ deps, pool: p, batchSize: 100, logger });

    expect(sweep.released).toBeGreaterThanOrEqual(1);
    expect(again.released).toBe(0);
    const { rows } = await p.query<{ id: string; status: string }>('SELECT id, status FROM holds WHERE id = ANY($1)', [
      [overdue, live],
    ]);
    const byId = Object.fromEntries(rows.map((r) => [r.id, r.status]));
    expect(byId[overdue]).toBe('EXPIRED');
    expect(byId[live]).toBe('ACTIVE');
    expect(await available(p, eventId)).toBe(7);
    const types = await p.query<{ event_type: string }>(
      `SELECT event_type FROM outbox WHERE aggregate_id = $1 ORDER BY created_at`,
      [overdue],
    );
    expect(types.rows.map((r) => r.event_type)).toEqual(['InventoryReserved', 'InventoryReleased', 'HoldExpired']);
  });
});

describe('waitlist (integration, real PostgreSQL)', () => {
  it('offers released tickets to the waitlist before anyone else can take them', async (ctx) => {
    const p = db(ctx);
    const deps = depsFor(p);
    const eventId = await createEvent(p, 2);
    const first = await reserve(deps, eventId, 2);

    const waiter = createId();
    const joined = await joinWaitlist(deps, {
      eventId,
      userId: waiter,
      quantity: 2,
      idempotencyKey: createId(),
      correlationId: createId(),
    });
    expect(joined.body).toMatchObject({ status: 'WAITING', position: 1 });

    const released = await releaseHold(deps, first.body.holdId, 'PAYMENT_FAILED');

    expect(released.offers).toHaveLength(1);
    expect(released.offers[0]!.userId).toBe(waiter);
    // Stock went straight from the released hold to the waitlisted user's hold.
    expect(await available(p, eventId)).toBe(0);
    const entry = await p.query<{ status: string; offered_hold_id: string }>(
      'SELECT status, offered_hold_id FROM waitlist WHERE id = $1',
      [joined.body.waitlistId],
    );
    expect(entry.rows[0]).toEqual({ status: 'OFFERED', offered_hold_id: released.offers[0]!.holdId });

    // And confirming the offered hold fulfils the waitlist entry.
    await confirmHold(deps, released.offers[0]!.holdId, createId());
    const after = await p.query<{ status: string }>('SELECT status FROM waitlist WHERE id = $1', [
      joined.body.waitlistId,
    ]);
    expect(after.rows[0]!.status).toBe('FULFILLED');
    await expectInventoryBalanced(p, eventId);
  });

  it('refuses to waitlist while tickets are still available', async (ctx) => {
    const p = db(ctx);
    const deps = depsFor(p);
    const eventId = await createEvent(p, 5);

    await expect(
      joinWaitlist(deps, {
        eventId,
        userId: createId(),
        quantity: 1,
        idempotencyKey: createId(),
        correlationId: createId(),
      }),
    ).rejects.toMatchObject({ code: 'TICKETS_AVAILABLE' });
  });
});
