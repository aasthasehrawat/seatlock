import { createHash } from 'node:crypto';
import { IdempotencyConflictError } from '@seatlock/shared';
import type { Queryable } from '../db/pool.js';

/**
 * IDEMPOTENT REQUESTS
 * -------------------
 * Clients retry (timeouts, double-clicks, flaky mobile networks). Without idempotency a retry of
 * "reserve 2 tickets" would take 4. With a client-supplied Idempotency-Key, the first request does
 * the work and records its response IN THE SAME TRANSACTION as the work; any retry with the same
 * key + same body replays that stored response without touching inventory.
 *
 * Same key + DIFFERENT body is a client bug -> 409 IDEMPOTENCY_CONFLICT (Stripe-style).
 */

export interface StoredResponse<T = unknown> {
  status: number;
  body: T;
}

/** Stable JSON: object keys sorted recursively so {a,b} and {b,a} hash identically. */
function canonicalize(value: unknown): unknown {
  if (Array.isArray(value)) return value.map(canonicalize);
  if (value !== null && typeof value === 'object') {
    return Object.fromEntries(
      Object.entries(value as Record<string, unknown>)
        .sort(([a], [b]) => (a < b ? -1 : a > b ? 1 : 0))
        .map(([k, v]) => [k, canonicalize(v)]),
    );
  }
  return value;
}

export function hashRequest(body: unknown): string {
  return createHash('sha256').update(JSON.stringify(canonicalize(body))).digest('hex');
}

/**
 * Keys are namespaced per operation so a key reused on /reservations and /waitlist
 * can never replay the wrong operation's response.
 */
export function scopedKey(scope: string, key: string): string {
  return `${scope}:${key}`;
}

/**
 * Serialise concurrent requests carrying the SAME key.
 *
 * Without this, two simultaneous first-time requests would both see "no record", both run the
 * business logic, and only the second INSERT would fail on the primary key (rolling back its work
 * and returning a 500 instead of the replayed response). A transaction-scoped advisory lock makes
 * the second request wait until the first commits, after which it finds the stored response.
 * The lock auto-releases at COMMIT/ROLLBACK. Different keys never contend.
 */
export async function lockKey(client: Queryable, key: string): Promise<void> {
  await client.query('SELECT pg_advisory_xact_lock(hashtextextended($1, 0))', [key]);
}

interface IdempotencyRow {
  request_hash: string;
  response_status: number;
  response_body: unknown;
}

/**
 * Returns the stored response if this key was already used with an identical request,
 * `null` if the key is new, and throws IdempotencyConflictError if the body differs.
 */
export async function findStoredResponse<T>(
  client: Queryable,
  key: string,
  requestHash: string,
): Promise<StoredResponse<T> | null> {
  const { rows } = await client.query<IdempotencyRow>(
    'SELECT request_hash, response_status, response_body FROM idempotency_keys WHERE key = $1',
    [key],
  );
  const row = rows[0];
  if (!row) return null;
  if (row.request_hash !== requestHash) throw new IdempotencyConflictError();
  return { status: row.response_status, body: row.response_body as T };
}

/** Persist the response. Must use the same client/transaction as the business write. */
export async function storeResponse(
  client: Queryable,
  key: string,
  requestHash: string,
  response: StoredResponse,
): Promise<void> {
  await client.query(
    `INSERT INTO idempotency_keys (key, request_hash, response_status, response_body)
     VALUES ($1, $2, $3, $4::jsonb)`,
    [key, requestHash, response.status, JSON.stringify(response.body)],
  );
}
