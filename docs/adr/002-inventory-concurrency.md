# ADR-002: Inventory concurrency control (never oversell)

- **Status:** Accepted
- **Date:** 2026-11-02
- **Deciders:** SeatLock maintainers
- **Related:** [ADR-001 outbox](001-transactional-outbox.md), [load test](../load-test.md)

## Context

A flash sale concentrates thousands of buyers on a small number of tickets in a few seconds. The one
non-negotiable business rule is: **never sell a ticket that does not exist.** Selling the same seat
twice is a refund, an apology and a reputation problem; selling one fewer than possible is merely a
missed sale.

The hot path is `POST /reservations`: many concurrent requests decrementing one counter
(`events.available`) for the same event.

Constraints:

- Correctness must not depend on Redis, on application-level locks, or on a single app instance.
- It must remain correct when requests are retried, when app servers crash mid-request, and when
  several app instances run at once.
- A reservation is a **hold** (a lease with a deadline), not a sale: stock must come back
  automatically if the customer never pays.

## Decision

Make the **database the single arbiter**, using one atomic conditional `UPDATE` as the only place
stock is ever decremented:

```sql
UPDATE events
   SET available = available - $qty
 WHERE id = $id AND available >= $qty
 RETURNING unit_price_cents;
```

and wrap the whole reservation in **one transaction** at the default isolation level (READ COMMITTED):

1. take a transaction-scoped advisory lock on the idempotency key and replay a stored response if one
   exists;
2. the conditional `UPDATE` above;
3. `rowCount = 0` → either the event does not exist (404) or there is not enough stock
   (`409 SOLD_OUT`); the transaction rolls back;
4. insert the hold (`ACTIVE`, `expires_at = NOW() + TTL`, using the **database** clock);
5. insert the `InventoryReserved` outbox row ([ADR-001](001-transactional-outbox.md));
6. insert the idempotency record;
7. `COMMIT`.

Implementation: `services/inventory-service/src/services/reservation.ts`.

### Why this cannot oversell

- **Check and write are one statement.** There is no gap between "is there enough?" and "take it" for
  another request to slip into. This eliminates the classic read-modify-write race of
  `SELECT available; if (available >= qty) UPDATE ...`.
- **Row lock + re-check.** `UPDATE` takes an exclusive row lock. Concurrent reservations for the same
  event queue behind it. Under READ COMMITTED, when the lock is released the waiting statement
  **re-evaluates its `WHERE` clause against the newly committed row version** (PostgreSQL's
  EvalPlanQual recheck). So request #101 for a 100-ticket event sees `available = 0`, matches zero
  rows, and gets `SOLD_OUT`. No lost updates, no negative stock.
- **Atomic with the hold.** The row lock is held until commit, so the decrement and the hold become
  visible together. If anything after step 2 fails (hold insert, outbox insert, crash), the
  transaction rolls back and the stock is restored automatically. There are no leaked tickets.
- **Defence in depth.** `events.available` has `CHECK (available >= 0)`. Even a future code bug that
  dropped the `WHERE` guard would abort the transaction instead of overselling.

### Returning stock safely (the other half)

Stock comes back through exactly one function (`releaseHoldInTx`), guarded twice:

1. the shared hold state machine (`ACTIVE` is the only state with outgoing edges), and
2. a row-level guard: `UPDATE holds SET status = ... WHERE id = $1 AND status = 'ACTIVE' RETURNING ...`.

Only the transaction that actually flipped the row proceeds to `available += quantity`. So the
following races each have exactly one winner and restore stock **at most once**:

- two expiry sweepers (or two instances) on the same hold;
- expiry vs. `PaymentFailed` vs. `OrderFailed` (a failed payment releases twice in practice);
- expiry vs. payment success (a confirmed hold is never released).

A post-update check refuses to commit if `available > total_tickets`, which would mean a double
restore.

Accounting invariant, per event:

```text
available + SUM(quantity of ACTIVE holds) + SUM(quantity of CONFIRMED holds) = total_tickets
```

Confirming a hold does not touch stock (it was deducted at reservation), so payment never competes
with other buyers for inventory.

### Supporting decisions

- **Idempotency keys.** `POST /reservations` requires `Idempotency-Key`. The first request records
  its response in the same transaction; retries with the same key and body replay it; the same key
  with a different body is `409 IDEMPOTENCY_CONFLICT`. A per-key advisory lock makes two simultaneous
  first-time requests queue instead of racing into a primary-key violation (which would surface as a
  500). Sold-out responses are deliberately **not** stored: the transaction rolls back, and a later
  retry (after a hold expires) may legitimately succeed.
- **Hold expiry** uses the database clock for both `expires_at` and the sweeper's comparison, so
  app-server clock skew cannot matter. A partial index on `ACTIVE` holds keeps the sweep cheap.
  A hold whose deadline passed a moment ago but was not yet swept is still confirmed if payment
  arrives: the stock is still ours and the customer has paid.
- **Waitlist** offers freed stock inside the same transaction that released it, while the event row is
  still locked, so ordinary buyers cannot snipe it. Offers are normal holds with the usual TTL.
- **Redis is not involved in correctness.** It only caches reads (5 s TTL) and rate-limits per IP;
  both fail open. A stale "3 left" can only lead to a clean `409 SOLD_OUT`.

## Consequences

### Positive

- **Provably correct under concurrency, crashes and retries,** with a very small amount of code and
  one primitive the database already guarantees.
- Works the same with one app instance or many.
- A single, easily explained invariant that tests can assert.

### Negative / costs

- **Reservations for one event serialise on one row.** That is the price of correctness. The critical
  section is a handful of indexed statements, but throughput for a single hot event is bounded by how
  fast Postgres can commit them one after another. [Load test](../load-test.md) is where to measure
  this; the result is not known until it has been run.
- **Connection pool pressure.** Waiting requests hold a pooled connection while they queue on the row
  lock. With `DB_POOL_MAX = 20`, `connectionTimeoutMillis = 5000` and `statement_timeout = 10000`, a
  large stampede can surface as 500s from pool timeouts rather than as `SOLD_OUT`. The load test
  checks for exactly this (`reservations_unexpected` must be 0).
- **Sold-out requests still cost a transaction** (the failed `UPDATE` plus one existence check); they
  are not rejected by a cheap in-memory counter.
- **Not horizontally shardable per event** without a design change (below).

### If a single event ever needs more throughput

Options, in the order we would try them:

1. **Bucketed inventory:** split `available` across N rows (for example 8 buckets) and decrement a
   randomly chosen bucket with the same guarded `UPDATE`, falling through to other buckets. Contention
   drops by about N; the invariant becomes a sum over buckets. Costs: sold-out detection must check
   all buckets, and fairness/ordering gets fuzzier.
2. **Admission control in front:** a virtual waiting room or a Redis counter as a _pre-filter_ that
   rejects obviously-sold-out traffic cheaply. Postgres stays authoritative; the counter may only ever
   say "no", never "yes".
3. **Raise pool size and tune Postgres** (measure first).

## Alternatives considered

| Alternative                                                                    | Verdict                                                                                                                                                                         |
| ------------------------------------------------------------------------------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `SELECT ... FOR UPDATE`, check in app code, then `UPDATE`                      | Also correct, but costs an extra round trip per request and holds the lock across application code. The guarded `UPDATE` is the cheapest correct primitive for a counter.       |
| `SERIALIZABLE` isolation                                                       | Correct, but turns contention into serialization failures that need retry loops; more complexity and wasted work under exactly the load we care about.                          |
| Optimistic concurrency (`version` column, retry on conflict)                   | Under a stampede, almost every attempt conflicts and retries: livelock-ish behaviour and a retry storm. Pessimistic locking on a hot row is better when contention is the norm. |
| Redis `DECR` / Lua as the source of truth                                      | Fast, but now inventory correctness depends on Redis durability and on keeping Redis and Postgres consistent (the dual-write problem again). A Redis failover could oversell.   |
| Distributed lock (Redlock, advisory lock per event) around a read-modify-write | More moving parts and new failure modes (lock expiry mid-operation). The row lock already is a lock, with transactional release.                                                |
| Message queue / single-writer actor per event                                  | Strong ordering and no row contention, but adds latency, an async "did I get it?" experience for the buyer and a stateful component to operate.                                 |
| Application-level check only                                                   | Incorrect by construction.                                                                                                                                                      |

## How we know it works

- `services/inventory-service/src/services/reservation.integration.test.ts` (real PostgreSQL via
  Testcontainers) asserts, among others: 300 concurrent single-ticket requests for 100 tickets give
  exactly 100 successes; mixed quantities never oversell; concurrent retries with the same
  `Idempotency-Key` create one hold; a sold-out attempt leaves no hold, outbox row or idempotency
  record; concurrent releases restore stock exactly once; a `CONFIRMED` hold is never released; the
  confirm-vs-expire race has exactly one winner and inventory balances.
- The k6 test in [`tests/load/reservation-load.js`](../../tests/load/reservation-load.js) repeats the
  claim at 1000 concurrent HTTP clients against the running service. Results are recorded in
  [load-test.md](../load-test.md) once it has been executed (**not yet executed**).
