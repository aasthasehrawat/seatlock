# ADR-001: Transactional outbox for publishing events

- **Status:** Accepted
- **Date:** 2026-11-02
- **Deciders:** SeatLock maintainers
- **Related:** [ADR-003 saga choreography](003-saga-choreography.md), [architecture](../architecture.md#4-the-transactional-outbox)

## Context

Every state change in SeatLock must be announced to other services: a reservation creates an
`InventoryReserved` event, an order confirmation creates `OrderConfirmed`, and so on. The services
are coordinated purely through Kafka ([ADR-003](003-saga-choreography.md)), so a missing or phantom
event is a correctness bug: a lost `InventoryReserved` means a customer holds tickets but never gets
an order; a phantom one means an order exists for stock that was never reserved.

The core difficulty is the **dual write**: a request handler must change Postgres **and** publish to
Kafka, and the two systems share no transaction.

| Naive approach                    | Failure                                                                      |
| --------------------------------- | ---------------------------------------------------------------------------- |
| Commit to Postgres, then publish  | Crash between the two: state changed, event **lost** forever.                |
| Publish to Kafka, then commit     | Commit fails or crash: event announced for a change that **never happened**. |
| Publish inside the DB transaction | Same as above; a Kafka ack does not make a later COMMIT succeed.             |
| Two-phase commit (XA) across both | Kafka does not participate in XA; operationally heavy, poor availability.    |

This matters most under the exact conditions a flash sale creates: bursts of traffic, deploys, and
brokers or pods restarting.

## Decision

Use the **transactional outbox** in every service that produces events (inventory, order, payment).

1. In the **same database transaction** as the business change, insert the event, as a complete
   envelope, into an `outbox` table (`published = false`). Either both rows commit or neither does.
2. A background **relay** in each service polls the outbox and publishes to Kafka:
   - `BEGIN`
   - `SELECT ... FROM outbox WHERE published = false ORDER BY created_at LIMIT n FOR UPDATE SKIP LOCKED`
   - `producer.send(batch)` with `acks = all` and an idempotent producer
   - `UPDATE outbox SET published = true ...`
   - `COMMIT`
3. The envelope (including its `eventId`) is created **once**, when written to the outbox. If the
   relay publishes a row twice, both copies carry the same `eventId`.
4. Delivery is therefore **at-least-once**. Every consumer is idempotent: it records
   `(consumer_name, eventId)` in `processed_events` in the same transaction as its own business change
   and skips duplicates.
5. Payloads are validated with the shared Zod schema **before** the outbox insert, so a malformed
   event aborts the producing transaction instead of poisoning every consumer.
6. Messages are keyed (concert id for inventory, order id for orders and payments) so events for one
   entity stay ordered within a partition.

Implementation: `services/inventory-service/src/lib/outbox.ts` and `workers/outbox-relay.ts`;
`services/order-service/src/workers/outbox-relay.ts` and `lib/outbox-events.ts`;
`services/payment-service/src/workers/outbox-relay.ts`.

Details that matter:

- **Why `FOR UPDATE SKIP LOCKED` (inventory, payment):** several relay instances can run side by side,
  each claiming a disjoint batch without blocking.
- **Why an advisory lock (order):** order-service allows exactly one relay to publish at a time, so
  events of one order go out in the order they were written. Extra replicas are hot standbys.
- **`clock_timestamp()` for `created_at` (inventory):** events written in one transaction, for example
  `InventoryReleased` then `WaitlistOffered`, keep their causal order when sorted. `NOW()` is frozen at
  transaction start and would tie them.
- **Partial index on unpublished rows:** the relay's scan stays tiny regardless of history size.
- **Cleanup (order):** published rows older than `OUTBOX_RETENTION_HOURS` (24) are purged
  periodically.

## Consequences

### Positive

- **No lost events and no phantom events.** The event exists if and only if the state change committed.
  A crash at any point leaves the row unpublished, and it is retried.
- **The request path never depends on Kafka.** `POST /reservations` keeps succeeding while Kafka is
  down; events accumulate in Postgres and drain when Kafka returns. (The Kafka-down scenario in
  [chaos-test.md](../chaos-test.md) is the procedure to confirm this on a running stack.)
- **Simple operational story.** "Is anything stuck?" is one query:
  `SELECT count(*) FROM outbox WHERE published = false`.
- **Auditable.** The outbox is a durable log of everything the service announced.

### Negative / costs

- **At-least-once, not exactly-once.** A crash between `send` and `markPublished` re-sends the batch.
  Every consumer must be idempotent; this is a permanent design constraint, not an optimisation.
- **Extra latency:** events leave after the next poll (default 500 ms), plus the relay's batching.
  Acceptable for a saga, wrong for sub-100 ms event fan-out.
- **Extra write load:** one more insert per event, and a hot, churning table that needs
  vacuum attention and eventual purging (only order-service purges today; see below).
- **Relay holds a transaction open during the Kafka send.** This is what prevents two relays from
  sending the same rows, but it means a slow broker holds locks and a connection. Batches are kept
  small for that reason.
- **Ordering is only per partition key**, and with several concurrent relays cross-batch order is
  best-effort (inventory). Consumers must tolerate reordering across keys.
- **Polling, not log-based.** It burns a few cheap queries per second per service even when idle.

### Known gaps

- Inventory and payment outboxes are not purged yet (order-service's is). They will grow without
  bound; add a retention job before running this for real.
- Header conventions on published messages differ per service (see the
  [event catalog](../event-catalog.md#kafka-message-headers)); consumers deliberately rely on the
  envelope body.

## Alternatives considered

| Alternative                                        | Why not                                                                                                                                                                                                           |
| -------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **CDC (Debezium) on the outbox table**             | Better latency and no polling, but adds Kafka Connect, Debezium and operational surface a four-service project does not need. The table design here makes it a drop-in later: replace the relay, keep the writes. |
| **Publish after commit, best effort**              | Loses events on crash; unacceptable for a saga.                                                                                                                                                                   |
| **Kafka transactions + consume-transform-produce** | Gives exactly-once only between Kafka topics; the state change is in Postgres, so it does not solve the dual write.                                                                                               |
| **Event sourcing**                                 | Solves the problem by making the log the source of truth, at a large cost in complexity and tooling.                                                                                                              |
| **Synchronous HTTP calls instead of events**       | Couples availability: one slow or down service fails the others. See ADR-003.                                                                                                                                     |

## How we know it works

- Relay unit tests exist in `services/inventory-service/src/workers/outbox-relay.test.ts` and
  `services/order-service/src/workers/outbox-relay.test.ts`.
- Order-service integration test (`order-flow.integration.test.ts`, real Postgres via Testcontainers):
  the consumer writes the order, the outbox row and the dedupe claim atomically; the relay publishes
  rows once; only one relay publishes at a time (advisory lock).
- Chaos scenarios 1 and 3 in [chaos-test.md](../chaos-test.md) (order-service down, Kafka down) are
  the end-to-end procedures; record their outcomes in that document's results log.
