import {
  EVENT_SCHEMAS,
  createEnvelope,
  createId,
  type EventEnvelope,
  type EventPayloadMap,
  type EventType,
} from '@seatlock/shared';
import type { Queryable } from '../db/pool.js';

/**
 * TRANSACTIONAL OUTBOX
 * --------------------
 * The dual-write problem: "update Postgres AND publish to Kafka" cannot be atomic, because
 * the two systems share no transaction. If we commit then crash before publishing, the event
 * is lost; if we publish then the commit fails, we announced something that never happened.
 *
 * Solution: write the event to an `outbox` table IN THE SAME TRANSACTION as the state change.
 * Either both commit or neither does. A separate relay (workers/outbox-relay.ts) later reads
 * committed rows and publishes them to Kafka (at-least-once). Consumers de-duplicate on the
 * envelope's eventId.
 */

export interface OutboxHeaders {
  correlationId: string;
  /** The concert (event) id. Also the Kafka partition key, so one concert is totally ordered. */
  eventId: string;
  type: EventType;
  /** Envelope message id: unique per message, what consumers use for idempotency. */
  messageId: string;
}

export interface EnqueueEventInput<K extends EventType> {
  /** What kind of entity changed, e.g. 'hold'. */
  aggregateType: string;
  /** Id of the entity that changed (hold id, ...). */
  aggregateId: string;
  type: K;
  payload: EventPayloadMap[K];
  /** Concert id: partition key for Kafka. */
  concertId: string;
  correlationId: string;
  producer: string;
}

/**
 * Insert an event into the outbox using the caller's transaction client.
 * MUST be called with the same client as the business write, never with the pool.
 *
 * The full envelope is stored as the payload, so the message id is minted exactly once:
 * if the relay publishes the same row twice (crash between send and mark), consumers see
 * the same eventId both times and can drop the duplicate.
 */
export async function enqueueEvent<K extends EventType>(
  client: Queryable,
  input: EnqueueEventInput<K>,
): Promise<EventEnvelope<EventPayloadMap[K]>> {
  // Fail fast at write time: a malformed event is a programmer error and must roll back
  // the business transaction rather than poison downstream consumers.
  const payload = EVENT_SCHEMAS[input.type].parse(input.payload) as EventPayloadMap[K];

  const envelope = createEnvelope({
    type: input.type,
    payload,
    producer: input.producer,
    correlationId: input.correlationId,
  });

  const headers: OutboxHeaders = {
    correlationId: envelope.correlationId,
    eventId: input.concertId,
    type: input.type,
    messageId: envelope.eventId,
  };

  // clock_timestamp() (wall clock, advances inside a transaction) instead of the default NOW()
  // (frozen at transaction start): events written in the same transaction, e.g.
  // InventoryReleased then WaitlistOffered, keep their causal order when the relay sorts by created_at.
  await client.query(
    `INSERT INTO outbox (id, aggregate_type, aggregate_id, event_type, payload, headers, created_at)
     VALUES ($1, $2, $3, $4, $5::jsonb, $6::jsonb, clock_timestamp())`,
    [
      createId(),
      input.aggregateType,
      input.aggregateId,
      input.type,
      JSON.stringify(envelope),
      JSON.stringify(headers),
    ],
  );

  return envelope;
}

export interface OutboxRow {
  id: string;
  aggregate_type: string;
  aggregate_id: string;
  event_type: EventType;
  payload: EventEnvelope;
  headers: Partial<OutboxHeaders>;
}

/**
 * Claim a batch of unpublished rows. FOR UPDATE SKIP LOCKED lets several relay instances
 * run side by side: each locks a disjoint batch, nobody blocks, nothing is published twice
 * by two live relays. Must run inside a transaction that stays open until `markPublished`.
 */
export async function claimUnpublished(client: Queryable, limit: number): Promise<OutboxRow[]> {
  const { rows } = await client.query<OutboxRow>(
    `SELECT id, aggregate_type, aggregate_id, event_type, payload, headers
       FROM outbox
      WHERE published = FALSE
      ORDER BY created_at, id
      LIMIT $1
      FOR UPDATE SKIP LOCKED`,
    [limit],
  );
  return rows;
}

export async function markPublished(client: Queryable, ids: string[]): Promise<void> {
  if (ids.length === 0) return;
  await client.query(
    `UPDATE outbox SET published = TRUE, published_at = NOW() WHERE id = ANY($1::uuid[])`,
    [ids],
  );
}
