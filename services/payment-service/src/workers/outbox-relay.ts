import type pg from 'pg';
import type { Producer } from 'kafkajs';
import type { Logger } from '../lib/logger.js';

interface OutboxRow {
  id: string;
  event_id: string;
  topic: string;
  message_key: string;
  event_type: string;
  payload: unknown;
}

export interface OutboxRelayOptions {
  pool: pg.Pool;
  producer: Producer;
  logger: Logger;
  pollIntervalMs: number;
  batchSize: number;
}

/**
 * Transactional-outbox relay: polls unpublished rows and sends them to Kafka.
 *
 * Rows are claimed with FOR UPDATE SKIP LOCKED, so several instances can run
 * side by side. If Kafka accepts the batch but the DB commit fails, the batch is
 * re-sent later (at-least-once); consumers dedupe on envelope.eventId.
 */
export class OutboxRelay {
  private timer: NodeJS.Timeout | null = null;
  private running = false;
  private inFlight: Promise<void> = Promise.resolve();

  constructor(private readonly options: OutboxRelayOptions) {}

  start(): void {
    if (this.running) return;
    this.running = true;
    this.schedule(0);
  }

  async stop(): Promise<void> {
    this.running = false;
    if (this.timer) {
      clearTimeout(this.timer);
      this.timer = null;
    }
    await this.inFlight;
  }

  /** Publish one batch. Returns the number of messages sent. Exposed for tests and manual flushes. */
  async publishBatch(): Promise<number> {
    const { pool, producer, batchSize } = this.options;
    const client = await pool.connect();
    try {
      await client.query('BEGIN');
      const { rows } = await client.query<OutboxRow>(
        `SELECT id, event_id, topic, message_key, event_type, payload
           FROM outbox
          WHERE published_at IS NULL
          ORDER BY id
          LIMIT $1
          FOR UPDATE SKIP LOCKED`,
        [batchSize],
      );

      if (rows.length === 0) {
        await client.query('COMMIT');
        return 0;
      }

      // Group by topic while preserving per-topic order.
      const byTopic = new Map<string, { key: string; value: string; headers: Record<string, string> }[]>();
      for (const row of rows) {
        const messages = byTopic.get(row.topic) ?? [];
        messages.push({
          key: row.message_key,
          value: JSON.stringify(row.payload),
          headers: { eventType: row.event_type, eventId: row.event_id },
        });
        byTopic.set(row.topic, messages);
      }

      await producer.sendBatch({
        topicMessages: [...byTopic.entries()].map(([topic, messages]) => ({ topic, messages })),
      });

      await client.query('UPDATE outbox SET published_at = NOW() WHERE id = ANY($1::bigint[])', [
        rows.map((r) => r.id),
      ]);
      await client.query('COMMIT');
      return rows.length;
    } catch (err) {
      try {
        await client.query('ROLLBACK');
      } catch {
        // ignore: original error is more useful
      }
      throw err;
    } finally {
      client.release();
    }
  }

  private schedule(delayMs: number): void {
    if (!this.running) return;
    this.timer = setTimeout(() => {
      this.timer = null;
      this.inFlight = this.tick();
    }, delayMs);
  }

  private async tick(): Promise<void> {
    let nextDelay = this.options.pollIntervalMs;
    try {
      const sent = await this.publishBatch();
      if (sent > 0) {
        this.options.logger.debug({ sent }, 'Published outbox batch');
      }
      // A full batch means there is probably more waiting: go again immediately.
      if (sent >= this.options.batchSize) {
        nextDelay = 0;
      }
    } catch (err) {
      this.options.logger.error({ err }, 'Outbox relay failed; will retry');
    } finally {
      this.schedule(nextDelay);
    }
  }
}
