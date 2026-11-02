import pg from 'pg';
import type { Logger } from '../lib/logger.js';

export function createPool(databaseUrl: string, logger?: Logger): pg.Pool {
  const pool = new pg.Pool({
    connectionString: databaseUrl,
    max: 10,
    idleTimeoutMillis: 30_000,
    connectionTimeoutMillis: 5_000,
  });

  // An idle client can error (e.g. DB restart). Without a listener Node would crash.
  pool.on('error', (err) => {
    logger?.error({ err }, 'Unexpected error on idle Postgres client');
  });

  return pool;
}
