import type { EventType } from '@seatlock/shared';
import type { OutboxHeaders, OutboxRecord, OutboxRepo, OutboxRow, Queryable } from './types.js';

interface DbOutboxRow {
  id: string;
  aggregate_type: string;
  aggregate_id: string;
  event_type: string;
  payload: unknown;
  headers: Partial<OutboxHeaders> | null;
  published: boolean;
  created_at: Date;
  published_at: Date | null;
}

/** Arbitrary constant identifying "the outbox relay" in pg advisory locks. */
const RELAY_LOCK_ID = 727_001;

function toRow(row: DbOutboxRow): OutboxRow {
  return {
    id: row.id,
    aggregateType: row.aggregate_type,
    aggregateId: row.aggregate_id,
    eventType: row.event_type as EventType,
    payload: row.payload,
    headers: row.headers ?? {},
    published: row.published,
    createdAt: row.created_at,
    publishedAt: row.published_at,
  };
}

export class PgOutboxRepo implements OutboxRepo {
  constructor(private readonly db: Queryable) {}

  async insert(record: OutboxRecord): Promise<void> {
    // clock_timestamp() (not NOW()) so created_at reflects the actual insert instant.
    // NOW() is the transaction *start* time, which can invert the order of two events for the
    // same order when a later-starting transaction wins the row lock first.
    await this.db.query(
      `INSERT INTO outbox (id, aggregate_type, aggregate_id, event_type, payload, headers, created_at)
       VALUES ($1, $2, $3, $4, $5::jsonb, $6::jsonb, clock_timestamp())`,
      [
        record.id,
        record.aggregateType,
        record.aggregateId,
        record.eventType,
        JSON.stringify(record.payload),
        JSON.stringify(record.headers),
      ],
    );
  }

  async tryAcquireRelayLock(): Promise<boolean> {
    const { rows } = await this.db.query<{ locked: boolean }>(
      'SELECT pg_try_advisory_xact_lock($1) AS locked',
      [RELAY_LOCK_ID],
    );
    return rows[0]?.locked === true;
  }

  async fetchUnpublished(limit: number): Promise<OutboxRow[]> {
    const { rows } = await this.db.query<DbOutboxRow>(
      `SELECT id, aggregate_type, aggregate_id, event_type, payload, headers,
              published, created_at, published_at
       FROM outbox
       WHERE published = FALSE
       ORDER BY created_at, id
       LIMIT $1`,
      [limit],
    );
    return rows.map(toRow);
  }

  async markPublished(ids: string[]): Promise<void> {
    if (ids.length === 0) return;
    await this.db.query(
      'UPDATE outbox SET published = TRUE, published_at = NOW() WHERE id = ANY($1::uuid[])',
      [ids],
    );
  }

  async purgePublishedBefore(cutoff: Date): Promise<number> {
    const result = await this.db.query(
      'DELETE FROM outbox WHERE published = TRUE AND published_at < $1',
      [cutoff],
    );
    return result.rowCount ?? 0;
  }
}
