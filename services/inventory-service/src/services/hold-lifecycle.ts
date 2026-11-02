import {
  EVENT_TYPES,
  InvalidStateTransitionError,
  NotFoundError,
  assertHoldTransition,
  canTransitionHold,
  createId,
  type HoldState,
  type ReleaseReason,
} from '@seatlock/shared';
import { withTransaction, type Queryable } from '../db/pool.js';
import { enqueueEvent } from '../lib/outbox.js';
import type { ServiceDeps } from './deps.js';
import { offerToNextWaitlisted, type WaitlistOffer } from './waitlist.js';

/**
 * HOLD LIFECYCLE
 *
 *            confirmHold                    releaseHold(reason)
 *   ACTIVE ------------------> CONFIRMED     ACTIVE ---------> RELEASED | EXPIRED
 *
 * Every state except ACTIVE is terminal (see canTransitionHold in @seatlock/shared).
 *
 * Inventory accounting invariant (per concert):
 *     available + SUM(quantity of ACTIVE holds) + SUM(quantity of CONFIRMED holds) = total_tickets
 * - reserve:  available -= q, hold ACTIVE
 * - confirm:  ACTIVE -> CONFIRMED, available unchanged (tickets were already deducted at reserve)
 * - release:  ACTIVE -> RELEASED/EXPIRED, available += q  (only ever done once per hold)
 *
 * "Released at most once, and never after confirmation" is guaranteed by TWO layers:
 *   1. the state machine (assertHoldTransition / canTransitionHold), and
 *   2. a row-level guard: `UPDATE holds ... WHERE id = $1 AND status = 'ACTIVE'`.
 *      Postgres row-locks the hold; a concurrent transaction blocks, then re-checks
 *      `status = 'ACTIVE'` against the committed result and matches nothing. Only the transaction
 *      that actually flipped the row proceeds to give the stock back, so double-release,
 *      expire-vs-confirm and expire-vs-payment-failed races all resolve to exactly one winner.
 */

// ---------------------------------------------------------------------------
// confirmHold
// ---------------------------------------------------------------------------

export interface ConfirmResult {
  holdId: string;
  eventId: string;
  quantity: number;
  /** false when the hold was already confirmed for this same order (idempotent replay). */
  confirmed: boolean;
}

interface HoldRow {
  id: string;
  event_id: string;
  quantity: number;
  status: HoldState;
  order_id: string | null;
}

/**
 * ACTIVE -> CONFIRMED. Inventory is NOT touched: it was deducted when the hold was created.
 *
 * - Already CONFIRMED for the same order -> no-op success (at-least-once message redelivery).
 * - EXPIRED / RELEASED / CONFIRMED-for-another-order -> InvalidStateTransitionError (409).
 * - An ACTIVE hold whose expires_at has just passed (sweeper hasn't run yet) is still confirmed:
 *   the customer has paid and the stock is still ours, so honouring the sale is the right call.
 *
 * Must run inside a transaction (the Kafka consumer composes it with processed_events).
 */
export async function confirmHoldInTx(
  client: Queryable,
  deps: Pick<ServiceDeps, 'serviceName'>,
  holdId: string,
  orderId: string,
  correlationId: string = createId(),
): Promise<ConfirmResult> {
  // Row lock: serialises against a concurrent release/expiry of the same hold.
  const { rows } = await client.query<HoldRow>(
    'SELECT id, event_id, quantity, status, order_id FROM holds WHERE id = $1 FOR UPDATE',
    [holdId],
  );
  const hold = rows[0];
  if (!hold) throw new NotFoundError('Hold', holdId);

  if (hold.status === 'CONFIRMED' && hold.order_id === orderId) {
    return { holdId, eventId: hold.event_id, quantity: hold.quantity, confirmed: false };
  }

  assertHoldTransition(hold.status, 'CONFIRMED');

  const updated = await client.query(
    `UPDATE holds
        SET status = 'CONFIRMED', order_id = $2, updated_at = NOW()
      WHERE id = $1 AND status = 'ACTIVE'`,
    [holdId, orderId],
  );
  if (updated.rowCount !== 1) {
    // Unreachable while we hold the row lock; kept as a loud invariant check.
    throw new InvalidStateTransitionError('hold', hold.status, 'CONFIRMED');
  }

  // If this hold was a waitlist offer, the offer is now fulfilled.
  await client.query(
    `UPDATE waitlist SET status = 'FULFILLED' WHERE offered_hold_id = $1 AND status = 'OFFERED'`,
    [holdId],
  );

  await enqueueEvent(client, {
    aggregateType: 'hold',
    aggregateId: holdId,
    type: EVENT_TYPES.InventoryConfirmed,
    concertId: hold.event_id,
    correlationId,
    producer: deps.serviceName,
    payload: { holdId, eventId: hold.event_id, orderId, quantity: hold.quantity },
  });

  return { holdId, eventId: hold.event_id, quantity: hold.quantity, confirmed: true };
}

export async function confirmHold(
  deps: ServiceDeps,
  holdId: string,
  orderId: string,
  correlationId?: string,
): Promise<ConfirmResult> {
  return withTransaction(deps.pool, (client) =>
    confirmHoldInTx(client, deps, holdId, orderId, correlationId),
  );
}

// ---------------------------------------------------------------------------
// releaseHold
// ---------------------------------------------------------------------------

export interface ReleaseResult {
  holdId: string;
  /** true only for the single caller that actually moved the hold out of ACTIVE. */
  released: boolean;
  /** Final state of the hold when `released` is false (why this call was a no-op). */
  currentStatus?: HoldState;
  eventId?: string;
  quantity?: number;
  /** Priority holds created for waitlisted users as a result of this release. */
  offers: WaitlistOffer[];
}

/**
 * ACTIVE -> RELEASED (or EXPIRED when reason is 'EXPIRED'), give the stock back, emit events,
 * then offer the freed tickets to the waitlist, all in one transaction.
 *
 * Idempotent and race-safe: if the hold is not ACTIVE any more (already released, expired by the
 * sweeper, or CONFIRMED because payment won the race) this is a harmless no-op that returns
 * `released: false` and, crucially, does NOT restore inventory a second time.
 *
 * Throws NotFoundError if the hold does not exist.
 */
export async function releaseHoldInTx(
  client: Queryable,
  deps: Pick<ServiceDeps, 'serviceName' | 'holdTtlSeconds'>,
  holdId: string,
  reason: ReleaseReason,
  correlationId: string = createId(),
): Promise<ReleaseResult> {
  const target: HoldState = reason === 'EXPIRED' ? 'EXPIRED' : 'RELEASED';
  // Layer 1: the state machine must allow leaving ACTIVE for this target.
  assertHoldTransition('ACTIVE', target);

  // Layer 2: row-level guard. Exactly one concurrent caller gets a row back.
  const flipped = await client.query<{ event_id: string; quantity: number }>(
    `UPDATE holds
        SET status = $2, updated_at = NOW()
      WHERE id = $1 AND status = 'ACTIVE'
      RETURNING event_id, quantity`,
    [holdId, target],
  );

  if (flipped.rowCount === 0) {
    const current = await client.query<{ status: HoldState }>(
      'SELECT status FROM holds WHERE id = $1',
      [holdId],
    );
    const status = current.rows[0]?.status;
    if (!status) throw new NotFoundError('Hold', holdId);
    if (canTransitionHold(status, target)) {
      // The state machine says this should have been possible, yet the guarded UPDATE matched
      // nothing: our model of the world is wrong. Fail loudly (rolls back) instead of guessing.
      throw new Error(`Invariant violated: hold ${holdId} is ${status} but release matched no row`);
    }
    return { holdId, released: false, currentStatus: status, offers: [] };
  }

  const { event_id: eventId, quantity } = flipped.rows[0]!;

  // Give the stock back. Safe without a guard on `available`: we hold the one-and-only
  // "flip" for this hold, so this += runs exactly once per hold.
  const restored = await client.query<{ available: number; total_tickets: number }>(
    `UPDATE events SET available = available + $2 WHERE id = $1
     RETURNING available, total_tickets`,
    [eventId, quantity],
  );
  const stock = restored.rows[0];
  if (!stock) throw new NotFoundError('Event', eventId);
  if (stock.available > stock.total_tickets) {
    // Would mean tickets were double-restored somewhere. Roll back rather than corrupt stock.
    throw new Error(
      `Invariant violated: available (${stock.available}) exceeds total (${stock.total_tickets}) for event ${eventId}`,
    );
  }

  // If this hold was a waitlist offer that nobody paid for, close that entry.
  await client.query(
    `UPDATE waitlist SET status = 'CANCELLED' WHERE offered_hold_id = $1 AND status = 'OFFERED'`,
    [holdId],
  );

  const common = {
    aggregateType: 'hold',
    aggregateId: holdId,
    concertId: eventId,
    correlationId,
    producer: deps.serviceName,
  } as const;

  await enqueueEvent(client, {
    ...common,
    type: EVENT_TYPES.InventoryReleased,
    payload: { holdId, eventId, quantity, reason },
  });
  if (reason === 'EXPIRED') {
    await enqueueEvent(client, {
      ...common,
      type: EVENT_TYPES.HoldExpired,
      payload: { holdId, eventId, quantity },
    });
  }

  // Same transaction, event row still locked: waitlisted users get first refusal.
  const offers = await offerToNextWaitlisted(client, deps, eventId, correlationId);

  return { holdId, released: true, eventId, quantity, offers };
}

export async function releaseHold(
  deps: ServiceDeps,
  holdId: string,
  reason: ReleaseReason,
  correlationId?: string,
): Promise<ReleaseResult> {
  return withTransaction(deps.pool, (client) =>
    releaseHoldInTx(client, deps, holdId, reason, correlationId),
  );
}
