import { pathToFileURL } from 'node:url';
import type pg from 'pg';
import { loadConfig } from '../config.js';
import { createLogger, type Logger } from '../lib/logger.js';
import { createPool } from './pool.js';

export const MIGRATION_SQL = `
CREATE TABLE IF NOT EXISTS payments (
  id UUID PRIMARY KEY,
  order_id UUID NOT NULL,
  hold_id UUID NOT NULL,
  user_id UUID NOT NULL,
  amount_cents INT NOT NULL CHECK (amount_cents >= 0),
  currency TEXT NOT NULL DEFAULT 'USD',
  status TEXT NOT NULL CHECK (status IN ('PENDING','SUCCEEDED','FAILED')),
  provider_ref TEXT,
  failure_reason TEXT,
  attempt_count INT NOT NULL DEFAULT 0,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE(order_id)
);

CREATE TABLE IF NOT EXISTS outbox (
  id BIGSERIAL PRIMARY KEY,
  event_id UUID NOT NULL UNIQUE,
  topic TEXT NOT NULL,
  message_key TEXT NOT NULL,
  event_type TEXT NOT NULL,
  payload JSONB NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  published_at TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_outbox_unpublished ON outbox (id) WHERE published_at IS NULL;

CREATE TABLE IF NOT EXISTS processed_events (
  event_id UUID PRIMARY KEY,
  event_type TEXT NOT NULL,
  processed_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
`;

/** Apply the schema. Safe to run repeatedly (all statements are IF NOT EXISTS). */
export async function migrate(pool: pg.Pool, logger?: Logger): Promise<void> {
  logger?.info('Running payment-service migrations');
  await pool.query(MIGRATION_SQL);
  logger?.info('Payment-service migrations complete');
}

async function main(): Promise<void> {
  const config = loadConfig();
  const logger = createLogger({ level: config.logLevel, name: config.serviceName });
  const pool = createPool(config.databaseUrl, logger);
  try {
    await migrate(pool, logger);
  } finally {
    await pool.end();
  }
}

// Run only when executed directly (npm run migrate), not when imported by index.ts.
if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  main().catch((err) => {
    console.error('Migration failed', err);
    process.exit(1);
  });
}
