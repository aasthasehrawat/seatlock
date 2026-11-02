import type { Producer } from 'kafkajs';
import type { UnitOfWork } from '../db/types.js';
import type { Logger } from '../lib/logger.js';
import { toKafkaMessage, type KafkaOutboundMessage } from '../lib/outbox-events.js';
import { sleep } from '../lib/sleep.js';

export interface OutboxRelayOptions {
  uow: UnitOfWork;
  /** Only `sendBatch` is used, which keeps this trivially fakeable in tests. */
  producer: Pick<Producer, 'sendBatch'>;
  logger: Logger;
  pollIntervalMs: number;
  batchSize: number;
  retentionHours: number;
  idempotencyTtlHours: number;
  /** How often to purge old rows. Defaults to 10 minutes. */
  cleanupIntervalMs?: number;
}

const MAX_BACKOFF_MS = 30_000;
const UNHEALTHY_AFTER_FAILURES = 5;

/**
 * Transactional-outbox relay: moves rows from the `outbox` table onto Kafka.
 *
 * Why an outbox? A service cannot atomically "update the DB AND publish to Kafka" (no shared
 * transaction). Writing the event to a table in the same DB transaction as the state change makes
 * that atomic; this relay then publishes it afterwards. A crash at any point leaves the row
 * unpublished, so it is retried.
 *
 * Delivery is AT-LEAST-ONCE: if we crash after Kafka acks but before marking the rows published,
 * they are sent again. That is fine because every consumer dedupes on the envelope eventId
 * (processed_events).
 *
 * Ordering: one transaction-scoped advisory lock lets only one relay publish at a time and rows
 * go out in (created_at, id) order, so events of one order reach its partition in the order they
 * were written. Extra replicas simply skip their turn; they are hot standbys, not extra throughput.
 *
 * The transaction (and the lock) stays open during the Kafka send. That is deliberate: the lock
 * is what stops two relays sending the same rows. Keep batches small so it is held briefly.
 */
export class OutboxRelay {
  private readonly log: Logger;
  private running = false;
  private consecutiveFailures = 0;
  private abort = new AbortController();
  private loopDone: Promise<void> = Promise.resolve();
  private lastCleanupAt = 0;

  constructor(private readonly opts: OutboxRelayOptions) {
    this.log = opts.logger.child({ component: 'outbox-relay' });
  }

  start(): void {
    if (this.running) return;
    this.running = true;
    this.abort = new AbortController();
    this.loopDone = this.loop();
    this.log.info({ pollIntervalMs: this.opts.pollIntervalMs }, 'outbox relay started');
  }

  /** Stop polling and wait for the in-flight batch (if any) to finish. */
  async stop(): Promise<void> {
    this.running = false;
    this.abort.abort();
    await this.loopDone;
    this.log.info('outbox relay stopped');
  }

  /** Healthy while running and not stuck in a failure streak (e.g. Kafka or DB down). */
  isHealthy(): boolean {
    return this.running && this.consecutiveFailures < UNHEALTHY_AFTER_FAILURES;
  }

  /**
   * Publish one batch. Returns the number of events published (0 if idle or if another relay
   * holds the lock). Exposed for tests and for a final flush on shutdown.
   */
  async runOnce(): Promise<number> {
    return this.opts.uow.run(async (ctx) => {
      if (!(await ctx.outbox.tryAcquireRelayLock())) return 0;

      const rows = await ctx.outbox.fetchUnpublished(this.opts.batchSize);
      if (rows.length === 0) return 0;

      const messages = rows.map(toKafkaMessage);
      await this.opts.producer.sendBatch({
        acks: -1,
        topicMessages: groupByTopic(messages).map(([topic, batch]) => ({
          topic,
          messages: batch.map((m) => ({ key: m.key, value: m.value, headers: m.headers })),
        })),
      });

      // If this UPDATE or the COMMIT fails after the send, the rows are re-sent (at-least-once).
      await ctx.outbox.markPublished(rows.map((r) => r.id));
      this.log.debug({ count: rows.length }, 'outbox batch published');
      return rows.length;
    });
  }

  private async loop(): Promise<void> {
    const { signal } = this.abort;

    while (this.running) {
      try {
        const published = await this.runOnce();
        this.consecutiveFailures = 0;

        // A full batch means there is probably more waiting: go again without sleeping.
        if (published >= this.opts.batchSize) continue;

        await this.maybeCleanup();
        await sleep(this.opts.pollIntervalMs, signal);
      } catch (err) {
        this.consecutiveFailures += 1;
        const backoff = Math.min(
          this.opts.pollIntervalMs * 2 ** this.consecutiveFailures,
          MAX_BACKOFF_MS,
        );
        this.log.error(
          { err, consecutiveFailures: this.consecutiveFailures, backoffMs: backoff },
          'outbox relay iteration failed',
        );
        await sleep(backoff, signal);
      }
    }
  }

  /** Published rows and expired idempotency keys are only useful for a while; keep tables small. */
  private async maybeCleanup(): Promise<void> {
    const interval = this.opts.cleanupIntervalMs ?? 10 * 60_000;
    if (Date.now() - this.lastCleanupAt < interval) return;
    this.lastCleanupAt = Date.now();

    try {
      const hour = 3_600_000;
      const outboxCutoff = new Date(Date.now() - this.opts.retentionHours * hour);
      const idempotencyCutoff = new Date(Date.now() - this.opts.idempotencyTtlHours * hour);

      const [outbox, keys] = await this.opts.uow.run<[number, number]>(async (ctx) => [
        await ctx.outbox.purgePublishedBefore(outboxCutoff),
        await ctx.idempotency.purgeBefore(idempotencyCutoff),
      ]);
      if (outbox > 0 || keys > 0) {
        this.log.info({ outboxRowsPurged: outbox, idempotencyKeysPurged: keys }, 'cleanup done');
      }
    } catch (err) {
      // Housekeeping must never take the relay down.
      this.log.warn({ err }, 'cleanup failed');
    }
  }
}

/** Group by topic, keeping first-seen topic order and in-topic message order. */
function groupByTopic(messages: KafkaOutboundMessage[]): Array<[string, KafkaOutboundMessage[]]> {
  const groups = new Map<string, KafkaOutboundMessage[]>();
  for (const message of messages) {
    const bucket = groups.get(message.topic);
    if (bucket) bucket.push(message);
    else groups.set(message.topic, [message]);
  }
  return [...groups.entries()];
}
