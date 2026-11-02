import type { Producer } from 'kafkajs';
import type { Logger } from 'pino';
import { TOPICS } from '@seatlock/shared';
import { withTransaction, type TxPool } from '../db/pool.js';
import { createPoller, type Poller } from '../lib/poller.js';
import { claimUnpublished, markPublished } from '../lib/outbox.js';

/**
 * OUTBOX RELAY
 * ------------
 * Moves committed outbox rows to Kafka. Delivery guarantee: AT-LEAST-ONCE.
 *
 * One relay pass, inside a single DB transaction:
 *   1. SELECT unpublished rows ... FOR UPDATE SKIP LOCKED   (claim a batch; other relays skip it)
 *   2. producer.send(...)                                    (wait for broker ack from all ISRs)
 *   3. UPDATE outbox SET published = TRUE                    (mark)
 *   4. COMMIT
 *
 * Failure analysis:
 *   - Kafka send fails        -> exception, ROLLBACK, rows stay unpublished, retried next tick.
 *   - Crash after send, before COMMIT -> rows still unpublished -> re-sent later: a DUPLICATE.
 *     This is the inherent at-least-once trade-off; consumers de-duplicate on the envelope's
 *     eventId (stored once in the outbox payload, so duplicates carry the same id).
 *   - Postgres down           -> nothing is claimed; business writes also fail, so nothing is lost.
 *
 * Ordering: Kafka only orders within a partition, so every message is keyed by the concert id.
 * All events for one concert go to one partition, in outbox created_at order, which is
 * what consumers care about (e.g. Reserved before Confirmed/Released for the same hold).
 * Caveat: with several relay instances running, batches are claimed by whichever instance gets
 * there first, so cross-batch ordering is best-effort; run a single relay (or partition the
 * outbox by key) if strict per-concert ordering under scale-out is ever required.
 */

export type RelayProducer = Pick<Producer, 'send'>;

export interface RelayDeps {
  pool: TxPool;
  producer: () => Promise<RelayProducer>;
  batchSize: number;
  logger: Logger;
}

/** Publish one batch. Returns the number of rows published. */
export async function relayOnce(deps: RelayDeps): Promise<number> {
  return withTransaction(deps.pool, async (client) => {
    const rows = await claimUnpublished(client, deps.batchSize);
    if (rows.length === 0) return 0;

    const producer = await deps.producer();
    await producer.send({
      topic: TOPICS.INVENTORY,
      acks: -1,
      messages: rows.map((row) => {
        const concertId = row.headers.eventId ?? row.aggregate_id;
        return {
          // Partition key = concert id.
          key: concertId,
          value: JSON.stringify(row.payload),
          headers: {
            correlationId: row.headers.correlationId ?? row.payload.correlationId,
            eventId: concertId,
            type: row.event_type,
            messageId: row.headers.messageId ?? row.payload.eventId,
          },
        };
      }),
    });

    await markPublished(
      client,
      rows.map((r) => r.id),
    );
    deps.logger.debug({ count: rows.length }, 'outbox batch published');
    return rows.length;
  });
}

export function createOutboxRelay(deps: RelayDeps & { intervalMs: number }): Poller {
  return createPoller({
    name: 'outbox-relay',
    intervalMs: deps.intervalMs,
    logger: deps.logger,
    // If we filled the batch there is probably more waiting: go again immediately.
    run: async () => (await relayOnce(deps)) >= deps.batchSize,
  });
}
