import pg from 'pg';
import type { Pool, PoolClient } from 'pg';
import type { Config } from '../config.js';
import type { Logger } from '../lib/logger.js';
import type { Queryable } from './types.js';

export function createPool(
  config: Pick<Config, 'databaseUrl' | 'dbPoolMax'>,
  logger: Logger,
): Pool {
  const pool = new pg.Pool({
    connectionString: config.databaseUrl,
    max: config.dbPoolMax,
    idleTimeoutMillis: 30_000,
    connectionTimeoutMillis: 5_000,
  });

  // An idle client erroring (e.g. DB restart) must not crash the process with an unhandled event.
  pool.on('error', (err) => {
    logger.error({ err }, 'unexpected error on idle postgres client');
  });

  return pool;
}

/** Narrow a pg Pool/PoolClient to the small `Queryable` interface the repositories use. */
export function toQueryable(source: Pool | PoolClient): Queryable {
  return {
    query: (text, values) => (source as Pool).query(text, values),
  };
}
