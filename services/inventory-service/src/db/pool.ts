import pg from 'pg';
import type { Pool, PoolClient } from 'pg';
import { config } from '../config.js';
import { logger } from '../lib/logger.js';

let pool: Pool | undefined;

/** Process-wide pg Pool singleton. */
export function getPool(): Pool {
  if (!pool) {
    pool = new pg.Pool({
      connectionString: config.databaseUrl,
      max: config.dbPoolMax,
      idleTimeoutMillis: 30_000,
      connectionTimeoutMillis: 5_000,
      // Safety net: no statement may hold row locks (e.g. on a hot event row) for long.
      statement_timeout: 10_000,
    });
    // An idle client erroring (e.g. DB restart) must not crash the process.
    pool.on('error', (err) => logger.error({ err }, 'unexpected idle pg client error'));
  }
  return pool;
}

export async function closePool(): Promise<void> {
  if (pool) {
    const p = pool;
    pool = undefined;
    await p.end();
  }
}

/** The only thing the services need from a pool: check out a client. Easy to mock in tests. */
export type TxPool = Pick<Pool, 'connect'>;

/** A client that can run queries: either a pooled client inside a transaction or the pool itself. */
export type Queryable = Pick<PoolClient, 'query'>;

/**
 * Run `fn` inside BEGIN/COMMIT on a dedicated connection, rolling back on any error.
 *
 * Default isolation (READ COMMITTED) is deliberate: inventory correctness relies on a
 * single atomic conditional UPDATE, not on snapshot isolation. See services/reservation.ts.
 */
export async function withTransaction<T>(
  txPool: TxPool,
  fn: (client: PoolClient) => Promise<T>,
): Promise<T> {
  const client = await txPool.connect();
  // If ROLLBACK itself fails the connection state is unknown; passing an Error to
  // release() makes the pool destroy the connection instead of reusing it.
  let discard: Error | undefined;
  try {
    await client.query('BEGIN');
    const result = await fn(client);
    await client.query('COMMIT');
    return result;
  } catch (err) {
    try {
      await client.query('ROLLBACK');
    } catch (rollbackErr) {
      discard = rollbackErr instanceof Error ? rollbackErr : new Error(String(rollbackErr));
      logger.error({ err: rollbackErr }, 'rollback failed; discarding connection');
    }
    throw err;
  } finally {
    client.release(discard);
  }
}
