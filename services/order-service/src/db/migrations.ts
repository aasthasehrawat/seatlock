/**
 * Migrations live in TypeScript (not .sql files) so `tsc` ships them in dist/ with no copy step.
 * Each migration runs once, in order, inside a transaction; applied ids are tracked in
 * `schema_migrations`. Statements are also written to be idempotent (IF NOT EXISTS).
 */
export interface Migration {
  id: string;
  sql: string;
}

export const MIGRATIONS: readonly Migration[] = [
  {
    id: '001_init',
    sql: `
CREATE TABLE IF NOT EXISTS orders (
  id UUID PRIMARY KEY,
  hold_id UUID NOT NULL UNIQUE,
  event_id UUID NOT NULL,
  user_id UUID NOT NULL,
  quantity INT NOT NULL CHECK (quantity > 0),
  amount_cents INT NOT NULL CHECK (amount_cents >= 0),
  status TEXT NOT NULL CHECK (status IN ('PENDING_PAYMENT','CONFIRMED','FAILED','EXPIRED')),
  payment_id UUID,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
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
`,
  },
  {
    id: '002_indexes',
    sql: `
-- "My orders" listing, newest first.
CREATE INDEX IF NOT EXISTS idx_orders_user_created ON orders (user_id, created_at DESC, id DESC);

-- The relay only ever scans unpublished rows; a partial index keeps that scan tiny
-- no matter how large the published history grows.
CREATE INDEX IF NOT EXISTS idx_outbox_unpublished ON outbox (created_at, id) WHERE published = FALSE;

-- Cleanup jobs delete by age.
CREATE INDEX IF NOT EXISTS idx_outbox_published_at ON outbox (published_at) WHERE published = TRUE;
CREATE INDEX IF NOT EXISTS idx_idempotency_created ON idempotency_keys (created_at);
`,
  },
];
