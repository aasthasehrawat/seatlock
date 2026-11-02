# ADR-003: Choreographed saga over event-driven services

- **Status:** Accepted
- **Date:** 2026-11-02
- **Deciders:** SeatLock maintainers
- **Related:** [ADR-001 outbox](001-transactional-outbox.md), [ADR-002 inventory concurrency](002-inventory-concurrency.md), [architecture](../architecture.md), [event catalog](../event-catalog.md)

## Context

Buying a ticket spans four responsibilities with four different data stores: reserving stock
(inventory), creating and tracking an order (order), charging money (payment) and telling the customer
(notification). There is no distributed transaction across them, so a purchase is a **saga**: a
sequence of local transactions, each with a compensating action if a later step fails.

Two ways to coordinate a saga:

- **Orchestration:** a central coordinator (a process manager or workflow engine) calls each step and
  decides what happens next.
- **Choreography:** no coordinator. Each service reacts to events and emits new ones; the overall flow
  emerges from who listens to what.

Requirements that shaped the choice:

1. Services must not fail because another service is down (a flash sale cannot stop taking
   reservations because the email service is restarting).
2. Four services and a short list of events: the flow is small and linear.
3. Every step must be safely retryable, because delivery is at-least-once
   ([ADR-001](001-transactional-outbox.md)).
4. The team is small; operational surface area matters.

## Decision

Use a **choreographed saga** over Kafka. Services never call each other over HTTP; the web app is the
only HTTP client. The saga:

| #   | Step (local transaction)                                             | Emits                                | Compensation if it fails later              |
| --- | -------------------------------------------------------------------- | ------------------------------------ | ------------------------------------------- |
| 1   | inventory: reserve stock, create `ACTIVE` hold                       | `InventoryReserved`                  | release hold, restore stock (`releaseHold`) |
| 2   | order: create order `PENDING_PAYMENT` (one per hold)                 | `OrderCreated`                       | order -> `EXPIRED` / `FAILED`               |
| 3   | order: customer calls pay; record intent                             | `PaymentRequested`                   | n/a (no money moved yet)                    |
| 4   | payment: charge (mock), record result                                | `PaymentSucceeded` / `PaymentFailed` | refund (**not implemented**, see below)     |
| 5   | order: `CONFIRMED` or `FAILED`; inventory: `CONFIRMED` or `RELEASED` | `OrderConfirmed` / `OrderFailed`     | n/a (terminal)                              |
| -   | inventory sweeper: hold TTL passes while still `ACTIVE`              | `HoldExpired`, `InventoryReleased`   | order -> `EXPIRED`                          |

Rules that make choreography safe here:

- **The reservation is the pivot.** Stock is deducted first and held under a lease. Everything after it
  either confirms the lease or lets it expire; nobody else can take those tickets in between.
- **Every handler is idempotent and order-tolerant.** Consumers dedupe on envelope `eventId`
  (`processed_events`, same transaction as the change), and shared state machines
  (`packages/shared/src/state`) reject illegal transitions, so late, duplicate or reordered events end
  as harmless no-ops.
- **Compensations are idempotent and race-safe.** A hold is released at most once and never after
  confirmation (guarded `UPDATE ... WHERE status = 'ACTIVE'`).
- **Each service owns its state machine and is the only writer of it.** Others express intent by
  emitting events, not by editing rows.
- **Time bounds everything.** The hold TTL (`HOLD_TTL_SECONDS`, default 300) guarantees the saga
  terminates even if a service never answers.
- **Failures are first-class.** Transient errors go to `retry.5s`; permanent ones to `dlq`, with
  tooling to inspect and replay.

## Consequences

### Positive

- **Loose coupling and independent availability.** order-service can be down and reservations keep
  working; the backlog is processed on restart (see [chaos scenario 1](../chaos-test.md)). Adding a
  consumer (analytics, ticket issuing) needs no change to producers; notification-service was added
  this way.
- **No single point of failure** and no orchestrator to scale or make highly available.
- **Simple per-service code:** "when X happens, do Y, emit Z".
- **Natural fit with the outbox.** Services already produce events; the saga is just those events.

### Negative / costs

- **The flow is implicit.** No single file shows "the purchase process". It is spread across four
  services' consumers. [architecture.md](../architecture.md) and the
  [event catalog](../event-catalog.md) exist to compensate; they must be kept up to date, because the
  code will not do it for you.
- **Harder debugging.** One purchase is a chain of asynchronous hops. Mitigation: a correlation id
  flows through HTTP (`X-Correlation-Id`), event envelopes and logs.
- **Eventual consistency is user-visible.** After `POST /reservations` the order does not exist yet;
  the web app polls `GET /orders?userId=` until it appears (it has a 20 s timeout and a friendly error).
- **Cyclic event dependencies are possible.** Inventory emits, order reacts, order emits, inventory
  reacts. Today the cycle terminates because compensations are idempotent no-ops (for example
  `OrderFailed` after `PaymentFailed` finds the hold already released). Anyone adding an event must
  check it cannot loop.
- **Changing the flow touches several services** and needs careful event-schema versioning.
- **Compensation is not always possible.** See gaps below.

### Known gaps (deliberate, recorded so they are not mistaken for oversights)

1. **No refund path.** If payment succeeds after the hold expired, the order is correctly `EXPIRED`
   (terminal), order-service logs "refund required", and inventory dead-letters the confirmation. A real
   system needs a compensating `RefundRequested` flow. Demonstrated in
   [chaos-test.md, section 8](../chaos-test.md#8-variation-outage-longer-than-the-hold-ttl).
2. **Waitlist offers do not create orders.** A waitlist offer creates a hold and emits
   `WaitlistOffered`, but only `InventoryReserved` triggers order creation in order-service, so an
   offered hold currently has no order to pay for. Either emit an event order-service understands, or
   let the offer carry the data order-service needs.
3. **Payment has no timeout / reconciliation.** If a payment result is lost, the order waits until the
   hold expires. There is no sweeper that asks payment-service for the status.

## Alternatives considered

| Alternative                                                                                                  | Why not (for now)                                                                                                                                                                                                                                                                                                                                |
| ------------------------------------------------------------------------------------------------------------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| **Orchestrated saga** (order-service or a new "saga" service drives steps, workflow engine such as Temporal) | Gives one place that shows the whole flow, easy timeouts and visibility, and is the better answer as flows grow (many steps, branching, long timers). For a four-step linear flow it adds a new component and a runtime dependency that is itself a SPOF. **Revisit** if the flow gains more than about 2 more steps or needs complex branching. |
| **Synchronous HTTP orchestration** (order calls inventory, then payment)                                     | Couples availability (any outage fails the purchase), needs timeouts, retries and circuit breakers everywhere, and still has no atomicity.                                                                                                                                                                                                       |
| **Two-phase commit / XA**                                                                                    | Not supported by Kafka; blocks on coordinator failure; poor fit for flash-sale traffic.                                                                                                                                                                                                                                                          |
| **Single service, single database (modular monolith)**                                                       | Honestly the simplest correct answer for this domain, with local ACID transactions. The microservice split here is a deliberate exercise in distributed-systems patterns (outbox, idempotency, sagas, DLQ). In production, start with the monolith unless team or scaling boundaries demanded otherwise.                                         |
| **Event sourcing for the whole system**                                                                      | Strong audit and replay, but a large conceptual and tooling cost beyond what the problem needs.                                                                                                                                                                                                                                                  |

## Notes for reviewers

- Choreography vs. orchestration is a trade-off, not a right answer. The honest summary: choreography
  buys decoupling and availability at the cost of a flow nobody can see in one place.
- The part that matters most for correctness is not the coordination style but the **local**
  guarantees: atomic stock decrement ([ADR-002](002-inventory-concurrency.md)), transactional outbox
  ([ADR-001](001-transactional-outbox.md)) and idempotent consumers. A choreographed saga is only safe
  when all three hold.
