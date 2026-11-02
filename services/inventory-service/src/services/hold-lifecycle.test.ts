import {
  HOLD_STATES,
  InvalidStateTransitionError,
  NotFoundError,
  assertHoldTransition,
  canTransitionHold,
  createId,
  type HoldState,
} from '@seatlock/shared';
import { describe, expect, it } from 'vitest';
import { createMockPool, type Responder } from '../test-support/mock-pool.js';
import type { ServiceDeps } from './deps.js';
import { confirmHold, releaseHold } from './hold-lifecycle.js';

const holdId = createId();
const eventId = createId();
const orderId = createId();

function setup(responder: Responder) {
  const mock = createMockPool(responder);
  const deps: ServiceDeps = { pool: mock.pool, holdTtlSeconds: 300, serviceName: 'inventory-service' };
  return { ...mock, deps };
}

const holdRow = (status: HoldState, order_id: string | null = null) => ({
  id: holdId,
  event_id: eventId,
  quantity: 3,
  status,
  order_id,
});

describe('hold state transitions (via shared state machine)', () => {
  it('only ACTIVE can move; CONFIRMED, EXPIRED and RELEASED are terminal', () => {
    const states = Object.values(HOLD_STATES);
    for (const from of states) {
      for (const to of states) {
        const expected = from === 'ACTIVE' && to !== 'ACTIVE';
        expect(canTransitionHold(from, to), `${from} -> ${to}`).toBe(expected);
      }
    }
  });

  it('a CONFIRMED hold can never be released or expired', () => {
    expect(() => assertHoldTransition('CONFIRMED', 'RELEASED')).toThrow(InvalidStateTransitionError);
    expect(() => assertHoldTransition('CONFIRMED', 'EXPIRED')).toThrow(InvalidStateTransitionError);
  });
});

describe('confirmHold', () => {
  it('moves ACTIVE -> CONFIRMED without touching inventory, and emits InventoryConfirmed', async () => {
    const t = setup((sql) => {
      if (sql.startsWith('SELECT id, event_id, quantity, status, order_id FROM holds')) {
        return { rows: [holdRow('ACTIVE')] };
      }
      if (sql.startsWith('UPDATE holds')) return { rowCount: 1 };
      return undefined;
    });

    const result = await confirmHold(t.deps, holdId, orderId, 'corr-1');

    expect(result).toEqual({ holdId, eventId, quantity: 3, confirmed: true });
    expect(t.has('FOR UPDATE')).toBe(true); // row lock on the hold
    const [update] = t.find('UPDATE holds');
    expect(update!.sql).toContain("status = 'ACTIVE'"); // row-level guard
    expect(t.has('UPDATE events')).toBe(false); // stock was already deducted at reservation
    expect(t.outboxEvents().map((e) => e.type)).toEqual(['InventoryConfirmed']);
    expect(t.outboxEvents()[0]!.payload).toMatchObject({ holdId, eventId, orderId, quantity: 3 });
    expect(t.has('COMMIT')).toBe(true);
  });

  it('is idempotent when the hold is already CONFIRMED for the same order', async () => {
    const t = setup((sql) =>
      sql.startsWith('SELECT id, event_id') ? { rows: [holdRow('CONFIRMED', orderId)] } : undefined,
    );

    const result = await confirmHold(t.deps, holdId, orderId);

    expect(result.confirmed).toBe(false);
    expect(t.has('UPDATE holds')).toBe(false);
    expect(t.has('INSERT INTO outbox')).toBe(false);
  });

  it.each(['EXPIRED', 'RELEASED'] as const)('refuses to confirm a %s hold', async (status) => {
    const t = setup((sql) =>
      sql.startsWith('SELECT id, event_id') ? { rows: [holdRow(status)] } : undefined,
    );

    await expect(confirmHold(t.deps, holdId, orderId)).rejects.toBeInstanceOf(InvalidStateTransitionError);
    expect(t.has('UPDATE holds')).toBe(false);
    expect(t.has('ROLLBACK')).toBe(true);
  });

  it('refuses to re-confirm a hold already confirmed for a DIFFERENT order', async () => {
    const t = setup((sql) =>
      sql.startsWith('SELECT id, event_id') ? { rows: [holdRow('CONFIRMED', createId())] } : undefined,
    );
    await expect(confirmHold(t.deps, holdId, orderId)).rejects.toBeInstanceOf(InvalidStateTransitionError);
  });

  it('throws NotFoundError for an unknown hold', async () => {
    const t = setup(() => undefined);
    await expect(confirmHold(t.deps, holdId, orderId)).rejects.toBeInstanceOf(NotFoundError);
  });
});

describe('releaseHold', () => {
  /** Happy-path responder: hold flips, stock restored to 7 of 10, empty waitlist. */
  const releasing: Responder = (sql) => {
    if (sql.startsWith('UPDATE holds')) return { rows: [{ event_id: eventId, quantity: 3 }] };
    if (sql.startsWith('UPDATE events SET available = available +')) {
      return { rows: [{ available: 7, total_tickets: 10 }] };
    }
    if (sql.startsWith('SELECT available FROM events')) return { rows: [{ available: 7 }] };
    return undefined;
  };

  it('moves ACTIVE -> RELEASED, restores stock once, emits InventoryReleased', async () => {
    const t = setup(releasing);

    const result = await releaseHold(t.deps, holdId, 'PAYMENT_FAILED', 'corr-1');

    expect(result).toMatchObject({ released: true, holdId, eventId, quantity: 3, offers: [] });
    const [flip] = t.find('UPDATE holds');
    expect(flip!.sql).toContain("status = 'ACTIVE'");
    expect(flip!.values).toEqual([holdId, 'RELEASED']);
    expect(t.find('UPDATE events SET available = available +')[0]!.values).toEqual([eventId, 3]);
    expect(t.outboxEvents().map((e) => e.type)).toEqual(['InventoryReleased']);
    expect(t.outboxEvents()[0]!.payload).toMatchObject({ reason: 'PAYMENT_FAILED', quantity: 3 });
  });

  it('uses EXPIRED state and also emits HoldExpired when the reason is EXPIRED', async () => {
    const t = setup(releasing);

    await releaseHold(t.deps, holdId, 'EXPIRED');

    expect(t.find('UPDATE holds')[0]!.values).toEqual([holdId, 'EXPIRED']);
    expect(t.outboxEvents().map((e) => e.type)).toEqual(['InventoryReleased', 'HoldExpired']);
  });

  it.each(['CONFIRMED', 'RELEASED', 'EXPIRED'] as const)(
    'is a no-op (no second restore) when the hold is already %s',
    async (status) => {
      const t = setup((sql) => {
        if (sql.startsWith('UPDATE holds')) return { rows: [], rowCount: 0 }; // guard matched nothing
        if (sql.startsWith('SELECT status FROM holds')) return { rows: [{ status }] };
        return undefined;
      });

      const result = await releaseHold(t.deps, holdId, 'EXPIRED');

      expect(result).toEqual({ holdId, released: false, currentStatus: status, offers: [] });
      expect(t.has('UPDATE events')).toBe(false);
      expect(t.has('INSERT INTO outbox')).toBe(false);
      expect(t.has('COMMIT')).toBe(true);
    },
  );

  it('throws NotFoundError for an unknown hold', async () => {
    const t = setup((sql) => (sql.startsWith('UPDATE holds') ? { rows: [], rowCount: 0 } : undefined));
    await expect(releaseHold(t.deps, holdId, 'RELEASED')).rejects.toBeInstanceOf(NotFoundError);
  });

  it('aborts (rolls back) if restoring would push available above total_tickets', async () => {
    const t = setup((sql) => {
      if (sql.startsWith('UPDATE holds')) return { rows: [{ event_id: eventId, quantity: 3 }] };
      if (sql.startsWith('UPDATE events SET available = available +')) {
        return { rows: [{ available: 11, total_tickets: 10 }] };
      }
      return undefined;
    });

    await expect(releaseHold(t.deps, holdId, 'RELEASED')).rejects.toThrow(/Invariant violated/);
    expect(t.has('ROLLBACK')).toBe(true);
    expect(t.has('INSERT INTO outbox')).toBe(false);
  });

  it('offers freed tickets to the next waitlisted user in the same transaction', async () => {
    const waiting = { id: createId(), user_id: createId(), quantity: 3 };
    let waitlistQueried = false;
    const t = setup((sql) => {
      if (sql.startsWith('UPDATE holds SET status')) return { rows: [{ event_id: eventId, quantity: 3 }] };
      if (sql.startsWith('UPDATE events SET available = available +')) {
        return { rows: [{ available: 3, total_tickets: 10 }] };
      }
      if (sql.startsWith('SELECT available FROM events')) return { rows: [{ available: 3 }] };
      if (sql.startsWith('SELECT id, user_id, quantity FROM waitlist')) {
        if (waitlistQueried) return { rows: [] };
        waitlistQueried = true;
        return { rows: [waiting] };
      }
      if (sql.startsWith('UPDATE events SET available = available -')) return { rows: [{ available: 0 }] };
      if (sql.startsWith('INSERT INTO holds')) return { rows: [{ expires_at: new Date('2027-01-01T00:05:00Z') }] };
      return undefined;
    });

    const result = await releaseHold(t.deps, holdId, 'EXPIRED');

    expect(result.offers).toHaveLength(1);
    expect(result.offers[0]).toMatchObject({ waitlistId: waiting.id, userId: waiting.user_id, quantity: 3 });
    expect(t.outboxEvents().map((e) => e.type)).toEqual([
      'InventoryReleased',
      'HoldExpired',
      'WaitlistOffered',
    ]);
    expect(t.find('SELECT id, user_id, quantity FROM waitlist')[0]!.sql).toContain('SKIP LOCKED');
    // One transaction for the whole release + offer.
    expect(t.calls.filter((c) => c.sql === 'BEGIN')).toHaveLength(1);
    expect(t.calls.filter((c) => c.sql === 'COMMIT')).toHaveLength(1);
  });
});
