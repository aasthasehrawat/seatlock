import type { Pool } from 'pg';
import type { Logger } from '../lib/logger.js';
import { MIGRATIONS, type Migration } from './migrations.js';

/** Arbitrary constant: all instances contend on the same advisory lock while migrating. */
const MIGRATION_LOCK_ID = 727_002;

/**
 * Apply pending migrations. Safe to call from every replica at boot: a session-level advisory
 * lock makes replicas queue up, and the loser finds everything already applied.
 * Returns the ids of migrations applied by this call.
 */
export async function runMigrations(
  pool: Pool,
  logger: Logger,
  migrations: readonly Migration[] = MIGRATIONS,
): Promise<string[]> {
  const client = await pool.connect();
  const applied: string[] = [];

  try {
    await client.query('SELECT pg_advisory_lock($1)', [MIGRATION_LOCK_ID]);
    await client.query(`
      CREATE TABLE IF NOT EXISTS schema_migrations (
        id TEXT PRIMARY KEY,
        applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
      )
    `);

    const done = await client.query<{ id: string }>('SELECT id FROM schema_migrations');
    const alreadyApplied = new Set(done.rows.map((r) => r.id));

    for (const migration of migrations) {
      if (alreadyApplied.has(migration.id)) continue;

      try {
        await client.query('BEGIN');
        await client.query(migration.sql);
        await client.query('INSERT INTO schema_migrations (id) VALUES ($1)', [migration.id]);
        await client.query('COMMIT');
      } catch (err) {
        await client.query('ROLLBACK');
        throw err;
      }

      applied.push(migration.id);
      logger.info({ migration: migration.id }, 'migration applied');
    }
  } finally {
    try {
      await client.query('SELECT pg_advisory_unlock($1)', [MIGRATION_LOCK_ID]);
    } finally {
      client.release();
    }
  }

  if (applied.length === 0) logger.info('database schema is up to date');
  return applied;
}
