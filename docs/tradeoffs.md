# Trade-offs and known gaps

Every design here is a choice with a price. This page lists the main ones, what we gave up, and what
would make us revisit. It is deliberately honest: a trade-off you cannot name is one you did not make
on purpose.

- [Design trade-offs](#design-trade-offs)
- [Known gaps](#known-gaps)
- [What we would do next](#what-we-would-do-next)

---

## Design trade-offs

### 1. Correctness first: one hot row per event

|                  |                                                                                                                                                                   |
| ---------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Chose**        | A single guarded `UPDATE events SET available = available - q WHERE available >= q` ([ADR-002](adr/002-inventory-concurrency.md)).                                |
| **Gave up**      | Parallelism on one event: concurrent buyers of the same event serialise on one row lock.                                                                          |
| **Why OK**       | The critical section is a few indexed statements; correctness is provable; the alternative failure (overselling) is worse than a slower sale.                     |
| **Revisit when** | The [load test](load-test.md) shows one event's throughput is the limit and the business needs more. Then: bucketed counters, or an admission-control pre-filter. |

### 2. Transactional outbox, at-least-once delivery

|                  |                                                                                                                              |
| ---------------- | ---------------------------------------------------------------------------------------------------------------------------- |
| **Chose**        | Events written in the same DB transaction as state changes, relayed by polling ([ADR-001](adr/001-transactional-outbox.md)). |
| **Gave up**      | Exactly-once delivery, a little latency (about 500 ms poll), an extra table and write per event. Duplicates are possible.    |
| **Why OK**       | Zero lost or phantom events; the request path never depends on Kafka; consumers are idempotent anyway.                       |
| **Revisit when** | Latency matters (move to CDC / Debezium) or relay polling load becomes visible.                                              |

### 3. Choreographed saga, no orchestrator

|                  |                                                                                                                           |
| ---------------- | ------------------------------------------------------------------------------------------------------------------------- |
| **Chose**        | Services react to each other's events ([ADR-003](adr/003-saga-choreography.md)).                                          |
| **Gave up**      | A single place that shows the purchase flow; easy global timeouts; easy debugging.                                        |
| **Why OK**       | Four services, linear flow, independent availability, no extra infrastructure.                                            |
| **Revisit when** | The flow grows past a few more steps, gains branching, or needs long timers. Consider an orchestrator or workflow engine. |

### 4. Holds (leases) with a TTL instead of selling immediately

|                  |                                                                                                                                                                                  |
| ---------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Chose**        | Reserve stock for `HOLD_TTL_SECONDS` (default 300 s), pay within the window, otherwise it returns to sale.                                                                       |
| **Gave up**      | Stock is "unavailable but not sold" for up to 5 minutes; bots or abandoners can hold tickets and make an event look sold out when it is not; a late payment is a refund problem. |
| **Why OK**       | It is the standard answer: it gives the buyer a fair, predictable checkout window and makes stock recoverable automatically. The waitlist soaks up the reclaimed stock.          |
| **Revisit when** | Abandonment is high: shorten TTL, limit holds per user or IP, add queueing / waiting room.                                                                                       |

### 5. Deduct stock at reservation, not at payment

|             |                                                                                                                     |
| ----------- | ------------------------------------------------------------------------------------------------------------------- |
| **Chose**   | Decrement on reserve; confirmation does not touch stock.                                                            |
| **Gave up** | Cannot "oversubscribe" holds to compensate for expected abandonment (airlines do).                                  |
| **Why OK**  | Payment can never fail for lack of stock, so there is no "you paid but we have no tickets" case in the normal path. |

### 6. Retry topic instead of in-place retry

|                  |                                                                                                                                                                                |
| ---------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| **Chose**        | Failed messages are re-published to `retry.5s`, then to `dlq` after the attempt limit (inventory and order services).                                                          |
| **Gave up**      | Strict ordering for retried messages (a retried message may be processed after later ones for the same key); extra topics and headers to manage.                               |
| **Why OK**       | In-place retry causes head-of-line blocking: one bad message stalls a whole partition. All handlers are idempotent and order-tolerant (state machines), so reordering is safe. |
| **Revisit when** | A flow appears that is truly order-sensitive and cannot tolerate reordering.                                                                                                   |

### 7. Redis as an optimisation only, failing open

|                  |                                                                                                                     |
| ---------------- | ------------------------------------------------------------------------------------------------------------------- |
| **Chose**        | Redis caches reads (5 s TTL) and rate-limits; everything falls back to Postgres.                                    |
| **Gave up**      | While Redis is down there is no rate limiting (a security/abuse trade-off), and reads load Postgres.                |
| **Why OK**       | A cache outage must not become a ticket-sales outage. Correctness never depended on Redis.                          |
| **Revisit when** | Abuse during a Redis outage is a real risk: fail closed for rate limiting with a small in-process fallback limiter. |

### 8. Short read-cache TTL with no invalidation

|             |                                                                                                                                                                                   |
| ----------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Chose**   | `GET /events` cached 5 s, never invalidated on sale.                                                                                                                              |
| **Gave up** | Displayed stock can lag by up to 5 s, so a buyer sees "3 left" and gets `409 SOLD_OUT`.                                                                                           |
| **Why OK**  | Invalidating on every sale would make the cache useless exactly at peak load; the authoritative check is the atomic update. The UI treats sold-out as a normal, friendly outcome. |

### 9. Fixed-window rate limiting per IP

|                  |                                                                                                                                    |
| ---------------- | ---------------------------------------------------------------------------------------------------------------------------------- |
| **Chose**        | Redis `INCR` + `EXPIRE` in one Lua script; 20 requests per 60 s per IP by default.                                                 |
| **Gave up**      | Boundary burst (up to 2x the limit across a window edge); unfair to many users behind one NAT; trivially bypassed by rotating IPs. |
| **Why OK**       | Cheap, atomic, one Redis round trip, adequate to blunt naive scripts.                                                              |
| **Side effect**  | A load generator on one machine is rate limited too; the [load test](load-test.md) requires raising `RATE_LIMIT_MAX`.              |
| **Revisit when** | Real bot traffic: per-user limits, sliding window / token bucket, proof-of-work or CAPTCHA, a waiting room.                        |

### 10. Idempotency keys stored in Postgres, scoped per operation

|             |                                                                                                                                                                             |
| ----------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Chose**   | Client-supplied `Idempotency-Key`, stored with the response in the same transaction as the work. Same key + same body replays; same key + different body is `409`.          |
| **Gave up** | Clients must generate and persist keys; storage grows; successful outcomes only (sold-out and validation errors are not cached, so a later retry may legitimately succeed). |
| **Why OK**  | Makes retries (timeouts, double clicks, flaky mobile networks) safe for money and stock.                                                                                    |

### 11. Polling for the order after reserving

|                  |                                                                                                               |
| ---------------- | ------------------------------------------------------------------------------------------------------------- |
| **Chose**        | After `POST /reservations` the web app polls `GET /orders?userId=` until the order created via Kafka appears. |
| **Gave up**      | A little UX latency, extra requests, a possible timeout message.                                              |
| **Why OK**       | Honest representation of eventual consistency; no WebSockets or server-push infrastructure.                   |
| **Revisit when** | Latency is visible to users: SSE/WebSocket push, or return the pending order id synchronously.                |

### 12. Services share a Kafka cluster and topic names, but not code paths

|             |                                                                                                                |
| ----------- | -------------------------------------------------------------------------------------------------------------- |
| **Chose**   | A shared package for schemas, topics and state machines; every service implements its own consumer and outbox. |
| **Gave up** | Duplication: the four services have four slightly different DLQ/retry/outbox implementations and header names. |
| **Why OK**  | Services stay independently changeable and the shared package stays small and dependency-free.                 |
| **Cost**    | Inconsistency (see gaps). A shared "messaging" library would fix it at the price of coupling.                  |

### 13. Mock payment provider

|             |                                                                                                                                                   |
| ----------- | ------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Chose**   | payment-service simulates a gateway with configurable latency (500 ms) and failure rate (20%).                                                    |
| **Gave up** | Realism: no webhooks, no 3-D Secure, no partial failures, no refunds, no reconciliation.                                                          |
| **Why OK**  | It exercises the success and failure branches of the saga deterministically (`POST /payments/:id/simulate` forces an outcome outside production). |

### 14. A microservice split for a small domain

|                 |                                                                                                                                                                                                                                                                                                          |
| --------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Chose**       | Four services with four databases.                                                                                                                                                                                                                                                                       |
| **Gave up**     | Local ACID transactions across the whole purchase, simple debugging, and low operational cost.                                                                                                                                                                                                           |
| **Honest take** | For a team this size a modular monolith would be simpler and perfectly adequate. The split is here because the goal is to demonstrate and exercise distributed-systems patterns (outbox, idempotency, saga, DLQ, chaos testing). In production I would split only along real team or scaling boundaries. |

---

## Known gaps

Things that are missing or inconsistent today. Each is a candidate follow-up, not a surprise.

| #   | Gap                                                                                                                                                                                             | Impact                                                                                                                                | Fix direction                                                                                                                                 |
| --- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------- |
| 1   | **No refund flow.** A payment that succeeds after the hold expired leaves a charged customer with an `EXPIRED` order; order-service logs "refund required", inventory dead-letters the confirm. | Customer charged without tickets (in a real provider).                                                                                | Compensating `RefundRequested` saga step; payment-service refund API.                                                                         |
| 2   | **Waitlist offers create holds but not orders.** Only `InventoryReserved` triggers order creation.                                                                                              | An offered hold has nothing to pay for in the current flow.                                                                           | Emit `InventoryReserved` for offers, or have order-service handle `WaitlistOffered`.                                                          |
| 3   | **No authentication or authorisation.** `userId` is supplied by the client in request bodies and query strings; anyone can list anyone's orders. CORS allows any origin on inventory.           | Fine for a demo; unacceptable for production.                                                                                         | Real auth (OIDC/JWT); derive `userId` from the token; lock down CORS.                                                                         |
| 4   | **Inconsistent DLQ / retry / outbox conventions** across services (different header names, only inventory and order have retry topics).                                                         | Tooling needs a translation layer (`scripts/dlq-replay.ts`); payment/notification do not use `retry.5s`.                              | Shared messaging helpers in `@seatlock/shared`.                                                                                               |
| 5   | **`retry.30s` and `notifications.events` are provisioned but unused.** `NotificationRequested` has a schema but no producer or consumer.                                                        | Dead infrastructure.                                                                                                                  | Use them or remove them.                                                                                                                      |
| 6   | **Outbox / idempotency tables are not purged** in inventory and payment services (order-service purges).                                                                                        | Unbounded growth over time.                                                                                                           | Retention job (as in order-service).                                                                                                          |
| 7   | **Only order-service loads `.env`.** Inventory, payment and notification read the process environment and otherwise use code defaults (which match `.env.example`).                             | Editing `.env` does not change inventory settings such as `RATE_LIMIT_MAX` or `HOLD_TTL_SECONDS`; they must be exported in the shell. | Add a shared env loader to every service.                                                                                                     |
| 8   | **Single Kafka broker, replication factor 1,** no Postgres replicas.                                                                                                                            | No durability or HA story beyond a laptop.                                                                                            | Out of scope; documented.                                                                                                                     |
| 9   | **No metrics or tracing.** Observability is structured logs plus a correlation id.                                                                                                              | Hard to see lag, outbox backlog or latency without running SQL by hand.                                                               | Prometheus metrics (outbox backlog, consumer lag, DLQ depth), OpenTelemetry tracing.                                                          |
| 10  | **No payment timeout or reconciliation.** A lost payment result waits for the hold to expire.                                                                                                   | Slow failure detection.                                                                                                               | Reconciliation sweeper that queries payment status.                                                                                           |
| 11  | **No abuse controls beyond per-IP rate limit** (no per-user hold cap, no bot detection).                                                                                                        | Hoarding tickets via holds.                                                                                                           | Per-user hold limits; waiting room.                                                                                                           |
| 12  | **Load and chaos results not yet recorded.** The tests and procedures exist; the numbers do not.                                                                                                | Performance claims would be speculation.                                                                                              | Run `npm run load-test` and the chaos scenarios, fill in [load-test.md](load-test.md) and the [chaos results log](chaos-test.md#results-log). |
| 13  | **Fixed-window limiter** allows boundary bursts.                                                                                                                                                | Up to 2x limit across a window edge.                                                                                                  | Sliding window or token bucket.                                                                                                               |
| 14  | **Shared package must be built** (`npm run build:shared`) before services run.                                                                                                                  | A fresh clone fails with module-not-found until built.                                                                                | `prepare`/`postinstall` script, or TS project references.                                                                                     |

---

## What we would do next

1. **Run and record** the load and chaos tests; replace every `TBD` in the docs with real numbers.
2. **Observability:** outbox backlog, consumer lag and DLQ depth as metrics with alerts.
3. **Close the money gap:** refunds for late payments and a reconciliation sweeper.
4. **Real identity** so `userId` is not client-supplied.
5. **Unify messaging conventions** (headers, DLQ, retry, outbox cleanup) in the shared package.
6. **Decide the fate of unused infrastructure** (`retry.30s`, `notifications.events`).
7. **Bucketed inventory** only if measurement shows the single-row design is the limit.
