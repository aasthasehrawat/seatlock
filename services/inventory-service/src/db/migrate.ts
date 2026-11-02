import '../lib/load-env.js';
import { pathToFileURL } from 'node:url';
import type { Pool } from 'pg';
import { closePool, getPool, withTransaction } from './pool.js';
import { logger } from '../lib/logger.js';

/**
 * Schema for the inventory service. Every statement is idempotent (IF NOT EXISTS),
 * so `npm run migrate` can be re-run safely.
 *
 * Key design points:
 *  - events.available CHECK (>= 0)  is the database's last line of defence against overselling:
 *    even a buggy code path cannot drive stock negative, the transaction would abort.
 *  - idx_holds_expires is a PARTIAL index: it only contains ACTIVE holds, so the expiry
 *    sweep stays tiny and fast no matter how many historical holds exist.
 *  - idx_outbox_unpublished is partial for the same reason (relay scans only the backlog).
 *  - processed_events(consumer_name, event_id) PK gives consumers exactly-once *effects*
 *    on top of at-least-once delivery.
 */
export const MIGRATION_SQL = `
CREATE TABLE IF NOT EXISTS events (
  id UUID PRIMARY KEY,
  name TEXT NOT NULL,
  description TEXT,
  venue TEXT,
  starts_at TIMESTAMPTZ NOT NULL,
  total_tickets INT NOT NULL CHECK (total_tickets >= 0),
  available INT NOT NULL CHECK (available >= 0),
  unit_price_cents INT NOT NULL CHECK (unit_price_cents >= 0),
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS holds (
  id UUID PRIMARY KEY,
  event_id UUID NOT NULL REFERENCES events(id),
  user_id UUID NOT NULL,
  quantity INT NOT NULL CHECK (quantity > 0),
  status TEXT NOT NULL CHECK (status IN ('ACTIVE','CONFIRMED','EXPIRED','RELEASED')),
  expires_at TIMESTAMPTZ NOT NULL,
  order_id UUID,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS waitlist (
  id UUID PRIMARY KEY,
  event_id UUID NOT NULL REFERENCES events(id),
  user_id UUID NOT NULL,
  quantity INT NOT NULL CHECK (quantity > 0),
  status TEXT NOT NULL CHECK (status IN ('WAITING','OFFERED','FULFILLED','CANCELLED')),
  priority INT NOT NULL DEFAULT 0,
  offered_hold_id UUID,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS idempotency_keys (
  key TEXT PRIMARY KEY,
  request_hash TEXT NOT NULL,
  response_status INT NOT NULL,
  response_body JSONB NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS outbox (
  id UUID PRIMARY KEY,
  aggregate_type TEXT NOT NULL,
  aggregate_id TEXT NOT NULL,
  event_type TEXT NOT NULL,
  payload JSONB NOT NULL,
  headers JSONB NOT NULL DEFAULT '{}',
  published BOOLEAN NOT NULL DEFAULT FALSE,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  published_at TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS processed_events (
  consumer_name TEXT NOT NULL,
  event_id UUID NOT NULL,
  processed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  PRIMARY KEY (consumer_name, event_id)
);

CREATE INDEX IF NOT EXISTS idx_holds_expires ON holds(expires_at) WHERE status = 'ACTIVE';
CREATE INDEX IF NOT EXISTS idx_outbox_unpublished ON outbox(created_at) WHERE published = FALSE;
CREATE INDEX IF NOT EXISTS idx_waitlist_event ON waitlist(event_id, priority, created_at) WHERE status = 'WAITING';
`;

/** Arbitrary constant: serialises concurrent migrators (CREATE TABLE IF NOT EXISTS can race). */
const MIGRATION_LOCK_ID = 727_001;

export async function runMigrations(pool: Pick<Pool, 'connect'> = getPool()): Promise<void> {
  await withTransaction(pool, async (client) => {
    await client.query('SELECT pg_advisory_xact_lock($1)', [MIGRATION_LOCK_ID]);
    await client.query(MIGRATION_SQL);
  });
}

// Run only when executed directly (`npm run migrate`), not when imported by tests.
const isMain = process.argv[1] !== undefined && import.meta.url === pathToFileURL(process.argv[1]).href;

if (isMain) {
  runMigrations()
    .then(() => {
      logger.info('migrations applied');
    })
    .catch((err) => {
      logger.error({ err }, 'migration failed');
      process.exitCode = 1;
    })
    .finally(() => closePool());
}
