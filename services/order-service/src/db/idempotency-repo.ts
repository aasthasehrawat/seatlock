import type { IdempotencyRecord, IdempotencyRepo, Queryable } from './types.js';

interface IdempotencyRow {
  key: string;
  request_hash: string;
  response_status: number;
  response_body: unknown;
}

export class PgIdempotencyRepo implements IdempotencyRepo {
  constructor(private readonly db: Queryable) {}

  async lock(key: string): Promise<void> {
    // Transaction-scoped: released on COMMIT/ROLLBACK. Two requests carrying the same key queue
    // here, so the second one reliably sees the first one's committed result.
    await this.db.query('SELECT pg_advisory_xact_lock(hashtextextended($1, 0))', [key]);
  }

  async find(key: string): Promise<IdempotencyRecord | null> {
    const { rows } = await this.db.query<IdempotencyRow>(
      'SELECT key, request_hash, response_status, response_body FROM idempotency_keys WHERE key = $1',
      [key],
    );
    const row = rows[0];
    return row
      ? {
          key: row.key,
          requestHash: row.request_hash,
          responseStatus: row.response_status,
          responseBody: row.response_body,
        }
      : null;
  }

  async save(record: IdempotencyRecord): Promise<void> {
    await this.db.query(
      `INSERT INTO idempotency_keys (key, request_hash, response_status, response_body)
       VALUES ($1, $2, $3, $4::jsonb)
       ON CONFLICT (key) DO NOTHING`,
      [record.key, record.requestHash, record.responseStatus, JSON.stringify(record.responseBody)],
    );
  }

  async purgeBefore(cutoff: Date): Promise<number> {
    const result = await this.db.query('DELETE FROM idempotency_keys WHERE created_at < $1', [
      cutoff,
    ]);
    return result.rowCount ?? 0;
  }
}
