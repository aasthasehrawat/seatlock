import { pathToFileURL } from 'node:url';
import type pg from 'pg';
import { loadConfig } from '../config.js';
import { createLogger, type Logger } from '../lib/logger.js';
import { createPool } from './pool.js';

export const MIGRATION_SQL = `
CREATE TABLE IF NOT EXISTS notifications (
  id UUID PRIMARY KEY,
  user_id UUID NOT NULL,
  channel TEXT NOT NULL CHECK (channel IN ('EMAIL','SMS','PUSH')),
  template TEXT NOT NULL,
  payload JSONB NOT NULL,
  status TEXT NOT NULL CHECK (status IN ('PENDING','SENT','FAILED')),
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_notifications_user_created ON notifications (user_id, created_at DESC);

CREATE TABLE IF NOT EXISTS processed_events (
  event_id UUID PRIMARY KEY,
  event_type TEXT NOT NULL,
  processed_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Order/payment events carry a holdId but not a userId. We learn who owns each hold
-- from events that do carry both (InventoryReserved, OrderCreated, ...).
CREATE TABLE IF NOT EXISTS hold_owners (
  hold_id UUID PRIMARY KEY,
  user_id UUID NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
`;

/** Apply the schema. Safe to run repeatedly (all statements are IF NOT EXISTS). */
export async function migrate(pool: pg.Pool, logger?: Logger): Promise<void> {
  logger?.info('Running notification-service migrations');
  await pool.query(MIGRATION_SQL);
  logger?.info('Notification-service migrations complete');
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
