import type { ProcessedEventsRepo, Queryable } from './types.js';

export class PgProcessedEventsRepo implements ProcessedEventsRepo {
  constructor(private readonly db: Queryable) {}

  async markProcessed(consumerName: string, eventId: string): Promise<boolean> {
    // The primary key (consumer_name, event_id) does the dedupe. If this insert happens in the
    // same transaction as the business write, "processed" and "applied" are atomic: either both
    // commit or neither does, which turns at-least-once delivery into effectively-once.
    const result = await this.db.query(
      `INSERT INTO processed_events (consumer_name, event_id)
       VALUES ($1, $2)
       ON CONFLICT (consumer_name, event_id) DO NOTHING`,
      [consumerName, eventId],
    );
    return result.rowCount === 1;
  }
}
