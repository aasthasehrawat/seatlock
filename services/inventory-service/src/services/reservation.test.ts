import { IdempotencyConflictError, NotFoundError, SoldOutError, createId } from '@seatlock/shared';
import { describe, expect, it } from 'vitest';
import { hashRequest } from '../lib/idempotency.js';
import { createMockPool, type Responder } from '../test-support/mock-pool.js';
import type { ServiceDeps } from './deps.js';
import { reserveTickets } from './reservation.js';

const eventId = createId();
const userId = createId();

function setup(responder: Responder) {
  const mock = createMockPool(responder);
  const deps: ServiceDeps = { pool: mock.pool, holdTtlSeconds: 300, serviceName: 'inventory-service' };
  const input = { eventId, userId, quantity: 2, idempotencyKey: 'key-1', correlationId: 'corr-1' };
  return { ...mock, deps, input };
}

const expiresAt = new Date('2027-01-01T00:05:00.000Z');

describe('reserveTickets (unit, mocked pool)', () => {
  it('reserves tickets: guarded decrement, hold, outbox event, idempotency record, COMMIT', async () => {
    const t = setup((sql) => {
      if (sql.startsWith('UPDATE events')) return { rows: [{ unit_price_cents: 8000 }] };
      if (sql.startsWith('INSERT INTO holds')) return { rows: [{ expires_at: expiresAt }] };
      return undefined;
    });

    const result = await reserveTickets(t.deps, t.input);

    expect(result.status).toBe(201);
    expect(result.replayed).toBe(false);
    expect(result.body).toMatchObject({
      eventId,
      userId,
      quantity: 2,
      status: 'ACTIVE',
      unitPriceCents: 8000,
      totalPriceCents: 16000,
      expiresAt: expiresAt.toISOString(),
    });

    // The decrement is ONE conditional statement (check + write are atomic).
    const [dec] = t.find('UPDATE events');
    expect(dec!.sql).toContain('available = available - $2');
    expect(dec!.sql).toContain('available >= $2');
    expect(dec!.values).toEqual([eventId, 2]);

    // Statement order inside the single transaction.
    const order = t.calls.map((c) => c.sql.split(' ').slice(0, 3).join(' '));
    expect(order[0]).toBe('BEGIN');
    expect(order.at(-1)).toBe('COMMIT');
    const idx = (prefix: string) => order.findIndex((o) => o.startsWith(prefix));
    expect(idx('UPDATE events')).toBeLessThan(idx('INSERT INTO holds'));
    expect(idx('INSERT INTO holds')).toBeLessThan(idx('INSERT INTO outbox'));
    expect(idx('INSERT INTO outbox')).toBeLessThan(idx('INSERT INTO idempotency_keys'));
    expect(idx('INSERT INTO idempotency_keys')).toBeLessThan(order.indexOf('COMMIT'));

    // Outbox event is written in the same transaction, with a schema-valid payload.
    const [event] = t.outboxEvents();
    expect(event!.type).toBe('InventoryReserved');
    expect(event!.payload).toMatchObject({ eventId, userId, quantity: 2, unitPriceCents: 8000 });
    expect(event!.payload.holdId).toBe(result.body.holdId);

    expect(t.has('ROLLBACK')).toBe(false);
    expect(t.client.release).toHaveBeenCalledTimes(1);
  });

  it('throws SoldOutError and rolls back without creating a hold or event when no row matches', async () => {
    const t = setup((sql) => {
      if (sql.startsWith('UPDATE events')) return { rows: [], rowCount: 0 };
      if (sql.startsWith('SELECT 1 FROM events')) return { rows: [{ '?column?': 1 }] };
      return undefined;
    });

    await expect(reserveTickets(t.deps, t.input)).rejects.toBeInstanceOf(SoldOutError);

    expect(t.has('ROLLBACK')).toBe(true);
    expect(t.has('COMMIT')).toBe(false);
    expect(t.has('INSERT INTO holds')).toBe(false);
    expect(t.has('INSERT INTO outbox')).toBe(false);
    expect(t.has('INSERT INTO idempotency_keys')).toBe(false); // sold-out is retryable, never cached
    expect(t.client.release).toHaveBeenCalledTimes(1);
  });

  it('throws NotFoundError when the event does not exist', async () => {
    const t = setup((sql) => {
      if (sql.startsWith('UPDATE events')) return { rows: [], rowCount: 0 };
      if (sql.startsWith('SELECT 1 FROM events')) return { rows: [], rowCount: 0 };
      return undefined;
    });

    await expect(reserveTickets(t.deps, t.input)).rejects.toBeInstanceOf(NotFoundError);
    expect(t.has('ROLLBACK')).toBe(true);
  });

  it('replays the stored response for a retried request without touching inventory', async () => {
    const stored = { holdId: createId(), status: 'ACTIVE', quantity: 2 };
    const t = setup((sql) => {
      if (sql.startsWith('SELECT request_hash')) {
        return {
          rows: [
            {
              request_hash: hashRequest({ eventId, userId, quantity: 2 }),
              response_status: 201,
              response_body: stored,
            },
          ],
        };
      }
      return undefined;
    });

    const result = await reserveTickets(t.deps, t.input);

    expect(result.replayed).toBe(true);
    expect(result.status).toBe(201);
    expect(result.body).toEqual(stored);
    expect(t.has('UPDATE events')).toBe(false);
    expect(t.has('INSERT INTO holds')).toBe(false);
    expect(t.has('INSERT INTO outbox')).toBe(false);
    expect(t.has('COMMIT')).toBe(true);
  });

  it('rejects the same Idempotency-Key reused with a different request body', async () => {
    const t = setup((sql) => {
      if (sql.startsWith('SELECT request_hash')) {
        return {
          rows: [
            {
              request_hash: hashRequest({ eventId, userId, quantity: 99 }),
              response_status: 201,
              response_body: {},
            },
          ],
        };
      }
      return undefined;
    });

    await expect(reserveTickets(t.deps, t.input)).rejects.toBeInstanceOf(IdempotencyConflictError);
    expect(t.has('UPDATE events')).toBe(false);
    expect(t.has('ROLLBACK')).toBe(true);
  });

  it('rolls back the decrement if a later step fails (no leaked tickets)', async () => {
    const t = setup((sql) => {
      if (sql.startsWith('UPDATE events')) return { rows: [{ unit_price_cents: 100 }] };
      if (sql.startsWith('INSERT INTO holds')) throw new Error('disk full');
      return undefined;
    });

    await expect(reserveTickets(t.deps, t.input)).rejects.toThrow('disk full');
    expect(t.has('ROLLBACK')).toBe(true);
    expect(t.has('COMMIT')).toBe(false);
  });
});

describe('hashRequest', () => {
  it('is independent of key order and sensitive to values', () => {
    expect(hashRequest({ a: 1, b: 2 })).toBe(hashRequest({ b: 2, a: 1 }));
    expect(hashRequest({ a: 1 })).not.toBe(hashRequest({ a: 2 }));
  });
});
