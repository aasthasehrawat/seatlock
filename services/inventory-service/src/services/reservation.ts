import { EVENT_TYPES, NotFoundError, SoldOutError, createId } from '@seatlock/shared';
import { withTransaction } from '../db/pool.js';
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

export interface ReserveInput {
  eventId: string;
  userId: string;
  quantity: number;
  /** Client-supplied Idempotency-Key header. */
  idempotencyKey: string;
  correlationId: string;
}

export interface ReservationResponse {
  holdId: string;
  eventId: string;
  userId: string;
  quantity: number;
  status: 'ACTIVE';
  /** ISO-8601 UTC. Pay before this instant or the tickets go back on sale. */
  expiresAt: string;
  unitPriceCents: number;
  totalPriceCents: number;
}

export interface ReserveResult extends StoredResponse<ReservationResponse> {
  /** True when this was a retry and the stored response was replayed. */
  replayed: boolean;
}

export const RESERVATION_IDEMPOTENCY_SCOPE = 'reservation';

/**
 * Atomically reserve `quantity` tickets for a user and return a time-limited hold.
 *
 * Everything below happens in ONE database transaction:
 *   0. lock the idempotency key       (serialise concurrent retries of the same request)
 *   1. look up the idempotency key    -> replay the stored response if present
 *   2. conditional UPDATE on events   (the ONLY place stock is decremented)
 *   3. rowCount === 0                 -> SoldOutError (or NotFoundError), transaction rolls back
 *   4. INSERT hold (ACTIVE, expires_at = now + TTL)
 *   5. INSERT outbox row (InventoryReserved)
 *   6. INSERT idempotency record
 *   7. COMMIT
 *
 * WHY THIS CANNOT OVERSELL
 * ------------------------
 * The decrement is a single statement:
 *
 *     UPDATE events SET available = available - $qty WHERE id = $id AND available >= $qty
 *
 * (a) The check ("is there enough?") and the write ("take it") are ONE atomic statement. There is
 *     no gap between a SELECT and an UPDATE for another request to slip into, which is exactly the
 *     classic read-modify-write race of `SELECT available; if (available >= qty) UPDATE ...`.
 * (b) UPDATE takes a row-level exclusive lock on the event row. Concurrent reservations for the
 *     same concert queue up behind that lock. Under READ COMMITTED, when the lock is released the
 *     waiting statement RE-EVALUATES its WHERE clause against the newly committed row version
 *     (PostgreSQL's "EvalPlanQual" recheck). So request #101 for a 100-ticket event sees
 *     available = 0, matches zero rows, and gets SoldOut. No lost updates, no negative stock.
 * (c) The row lock is held until COMMIT/ROLLBACK, so the decrement and the hold INSERT become
 *     visible together. If anything after step 2 fails (hold insert, outbox insert, crash), the
 *     transaction rolls back and the stock is restored automatically: no leaked tickets.
 * (d) Defence in depth: events.available has CHECK (available >= 0). Even a future code bug that
 *     removed the WHERE guard would abort the transaction instead of overselling.
 *
 * Trade-off: reservations for one concert serialise on one row. That is deliberate (correctness
 * first), and the critical section is only a few indexed statements. If a single concert ever
 * needed more throughput, shard the counter across N rows (bucketed inventory).
 *
 * Why not SERIALIZABLE or SELECT ... FOR UPDATE? Both work, but cost retries / an extra round trip.
 * A guarded UPDATE is the cheapest correct primitive for a counter.
 *
 * Sold-out responses are intentionally NOT stored under the idempotency key: the transaction rolls
 * back, so a retry later (after a hold expires) may legitimately succeed.
 */
export async function reserveTickets(deps: ServiceDeps, input: ReserveInput): Promise<ReserveResult> {
  const key = scopedKey(RESERVATION_IDEMPOTENCY_SCOPE, input.idempotencyKey);
  const requestHash = hashRequest({
    eventId: input.eventId,
    userId: input.userId,
    quantity: input.quantity,
  });

  return withTransaction(deps.pool, async (client): Promise<ReserveResult> => {
    // 0 + 1. Idempotency: replay if we have seen this request before.
    await lockKey(client, key);
    const stored = await findStoredResponse<ReservationResponse>(client, key, requestHash);
    if (stored) return { ...stored, replayed: true };

    // 2. The atomic conditional decrement. RETURNING gives us the price in the same round trip.
    const dec = await client.query<{ unit_price_cents: number }>(
      `UPDATE events
          SET available = available - $2
        WHERE id = $1 AND available >= $2
        RETURNING unit_price_cents`,
      [input.eventId, input.quantity],
    );

    // 3. Zero rows matched: either the concert does not exist or there is not enough stock.
    //    We only pay for this disambiguating query on the failure path.
    if (dec.rowCount === 0) {
      const exists = await client.query('SELECT 1 FROM events WHERE id = $1', [input.eventId]);
      if (exists.rowCount === 0) throw new NotFoundError('Event', input.eventId);
      throw new SoldOutError();
    }
    const unitPriceCents = dec.rows[0]!.unit_price_cents;

    // 4. Create the hold. The expiry is computed with the DATABASE clock (NOW()) so it is
    //    consistent with the expiry worker's `expires_at < NOW()` comparison regardless of
    //    clock skew between app servers.
    const holdId = createId();
    const holdInsert = await client.query<{ expires_at: Date }>(
      `INSERT INTO holds (id, event_id, user_id, quantity, status, expires_at)
       VALUES ($1, $2, $3, $4, 'ACTIVE', NOW() + make_interval(secs => $5))
       RETURNING expires_at`,
      [holdId, input.eventId, input.userId, input.quantity, deps.holdTtlSeconds],
    );
    const expiresAt = new Date(holdInsert.rows[0]!.expires_at).toISOString();

    // 5. Transactional outbox: the event commits atomically with the stock decrement and the hold.
    await enqueueEvent(client, {
      aggregateType: 'hold',
      aggregateId: holdId,
      type: EVENT_TYPES.InventoryReserved,
      concertId: input.eventId,
      correlationId: input.correlationId,
      producer: deps.serviceName,
      payload: {
        holdId,
        eventId: input.eventId,
        userId: input.userId,
        quantity: input.quantity,
        unitPriceCents,
        expiresAt,
        // One reservation == one hold, so the hold id doubles as the reservation id.
        reservationId: holdId,
      },
    });

    const response: StoredResponse<ReservationResponse> = {
      status: 201,
      body: {
        holdId,
        eventId: input.eventId,
        userId: input.userId,
        quantity: input.quantity,
        status: 'ACTIVE',
        expiresAt,
        unitPriceCents,
        totalPriceCents: unitPriceCents * input.quantity,
      },
    };

    // 6. Record the response so retries replay it. Same transaction as everything above.
    await storeResponse(client, key, requestHash, response);

    // 7. COMMIT happens in withTransaction when this function returns.
    return { ...response, replayed: false };
  });
}
