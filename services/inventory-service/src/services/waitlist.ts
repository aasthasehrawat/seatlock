import { AppError, EVENT_TYPES, NotFoundError, createId } from '@seatlock/shared';
import { withTransaction, type Queryable } from '../db/pool.js';
import {
  findStoredResponse,
  hashRequest,
  lockKey,
  scopedKey,
  storeResponse,
  type StoredResponse,
} from '../lib/idempotency.js';
import { enqueueEvent } from '../lib/outbox.js';
import type { ServiceDeps } from './deps.js';

export const WAITLIST_IDEMPOTENCY_SCOPE = 'waitlist';

/** Upper bound on holds created by one release, so a huge release can't make one tx unbounded. */
const MAX_OFFERS_PER_PASS = 25;

export interface JoinWaitlistInput {
  eventId: string;
  userId: string;
  quantity: number;
  idempotencyKey: string;
  correlationId: string;
}

export interface WaitlistResponse {
  waitlistId: string;
  eventId: string;
  userId: string;
  quantity: number;
  status: 'WAITING';
  /** 1-based place in line (priority first, then arrival time). */
  position: number;
}

export interface JoinWaitlistResult extends StoredResponse<WaitlistResponse> {
  replayed: boolean;
}

/**
 * Put a user in line for a sold-out concert.
 *
 * Guard rails:
 *  - Only allowed when the requested quantity is NOT currently available. Otherwise the user
 *    should simply reserve; queueing while stock exists would let people jump ahead unfairly
 *    (offers only happen when inventory is released, never when someone joins).
 *  - A user can have only one WAITING entry per concert (advisory-locked check-then-insert).
 */
export async function joinWaitlist(
  deps: ServiceDeps,
  input: JoinWaitlistInput,
): Promise<JoinWaitlistResult> {
  const key = scopedKey(WAITLIST_IDEMPOTENCY_SCOPE, input.idempotencyKey);
  const requestHash = hashRequest({
    eventId: input.eventId,
    userId: input.userId,
    quantity: input.quantity,
  });

  return withTransaction(deps.pool, async (client): Promise<JoinWaitlistResult> => {
    await lockKey(client, key);
    const stored = await findStoredResponse<WaitlistResponse>(client, key, requestHash);
    if (stored) return { ...stored, replayed: true };

    // Serialise "does this user already wait?" check-then-insert for the same (event, user).
    await lockKey(client, `waitlist-member:${input.eventId}:${input.userId}`);

    const ev = await client.query<{ available: number }>(
      'SELECT available FROM events WHERE id = $1',
      [input.eventId],
    );
    if (ev.rowCount === 0) throw new NotFoundError('Event', input.eventId);
    if (ev.rows[0]!.available >= input.quantity) {
      throw new AppError(
        409,
        'TICKETS_AVAILABLE',
        'Tickets are currently available for this quantity; reserve them instead of joining the waitlist',
      );
    }

    const existing = await client.query(
      `SELECT 1 FROM waitlist WHERE event_id = $1 AND user_id = $2 AND status = 'WAITING'`,
      [input.eventId, input.userId],
    );
    if ((existing.rowCount ?? 0) > 0) {
      throw new AppError(409, 'ALREADY_WAITLISTED', 'User is already on the waitlist for this event');
    }

    const waitlistId = createId();
    await client.query(
      `INSERT INTO waitlist (id, event_id, user_id, quantity, status)
       VALUES ($1, $2, $3, $4, 'WAITING')`,
      [waitlistId, input.eventId, input.userId, input.quantity],
    );

    // Position = 1 + number of WAITING entries that sort before us. The row-value comparison
    // matches the ORDER BY used when offering and is served by idx_waitlist_event. We compare
    // against the stored row (not a JS Date) to keep Postgres' microsecond precision.
    const ahead = await client.query<{ ahead: string }>(
      `SELECT COUNT(*)::text AS ahead
         FROM waitlist w, (SELECT priority, created_at FROM waitlist WHERE id = $2) me
        WHERE w.event_id = $1 AND w.status = 'WAITING' AND w.id <> $2
          AND (w.priority, w.created_at) < (me.priority, me.created_at)`,
      [input.eventId, waitlistId],
    );

    const response: StoredResponse<WaitlistResponse> = {
      status: 201,
      body: {
        waitlistId,
        eventId: input.eventId,
        userId: input.userId,
        quantity: input.quantity,
        status: 'WAITING',
        position: Number(ahead.rows[0]!.ahead) + 1,
      },
    };
    await storeResponse(client, key, requestHash, response);
    return { ...response, replayed: false };
  });
}

export interface WaitlistOffer {
  waitlistId: string;
  holdId: string;
  userId: string;
  quantity: number;
  expiresAt: string;
}

/**
 * Turn freshly released inventory into priority holds for people waiting in line.
 *
 * Call this INSIDE the transaction that released the inventory (see hold-lifecycle.ts).
 * Because the event row stays locked from the release until COMMIT, no ordinary reservation can
 * snipe the tickets in between: waitlisted users get first refusal. Released stock that nobody
 * on the waitlist can use simply stays available.
 *
 * Selection policy: lowest `priority` value first, then oldest entry, among entries whose
 * quantity FITS the remaining stock (so one big party cannot block everyone behind it;
 * the trade-off is that large parties may wait longer).
 *
 * `FOR UPDATE SKIP LOCKED` lets concurrent releases pick different candidates without blocking.
 *
 * Each offer is a normal ACTIVE hold with the usual TTL; if the user does not pay in time it
 * expires and the tickets flow to the next person in line, automatically.
 */
export async function offerToNextWaitlisted(
  client: Queryable,
  deps: Pick<ServiceDeps, 'holdTtlSeconds' | 'serviceName'>,
  eventId: string,
  correlationId: string,
): Promise<WaitlistOffer[]> {
  // Lock the event row (re-entrant if the caller already holds it) and read stock.
  const ev = await client.query<{ available: number }>(
    'SELECT available FROM events WHERE id = $1 FOR UPDATE',
    [eventId],
  );
  if (ev.rowCount === 0) return [];
  let available = ev.rows[0]!.available;

  const offers: WaitlistOffer[] = [];

  while (available > 0 && offers.length < MAX_OFFERS_PER_PASS) {
    const next = await client.query<{ id: string; user_id: string; quantity: number }>(
      `SELECT id, user_id, quantity
         FROM waitlist
        WHERE event_id = $1 AND status = 'WAITING' AND quantity <= $2
        ORDER BY priority, created_at
        LIMIT 1
        FOR UPDATE SKIP LOCKED`,
      [eventId, available],
    );
    const entry = next.rows[0];
    if (!entry) break;

    // Same guarded decrement as a normal reservation.
    const dec = await client.query<{ available: number }>(
      `UPDATE events SET available = available - $2
        WHERE id = $1 AND available >= $2
        RETURNING available`,
      [eventId, entry.quantity],
    );
    if (dec.rowCount === 0) break;
    available = dec.rows[0]!.available;

    const holdId = createId();
    const hold = await client.query<{ expires_at: Date }>(
      `INSERT INTO holds (id, event_id, user_id, quantity, status, expires_at)
       VALUES ($1, $2, $3, $4, 'ACTIVE', NOW() + make_interval(secs => $5))
       RETURNING expires_at`,
      [holdId, eventId, entry.user_id, entry.quantity, deps.holdTtlSeconds],
    );
    const expiresAt = new Date(hold.rows[0]!.expires_at).toISOString();

    await client.query(
      `UPDATE waitlist SET status = 'OFFERED', offered_hold_id = $2 WHERE id = $1`,
      [entry.id, holdId],
    );

    await enqueueEvent(client, {
      aggregateType: 'waitlist',
      aggregateId: entry.id,
      type: EVENT_TYPES.WaitlistOffered,
      concertId: eventId,
      correlationId,
      producer: deps.serviceName,
      payload: {
        waitlistId: entry.id,
        eventId,
        userId: entry.user_id,
        quantity: entry.quantity,
        holdId,
        expiresAt,
      },
    });

    offers.push({
      waitlistId: entry.id,
      holdId,
      userId: entry.user_id,
      quantity: entry.quantity,
      expiresAt,
    });
  }

  return offers;
}
