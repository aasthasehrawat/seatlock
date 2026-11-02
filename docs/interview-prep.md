# Interview prep: explaining SeatLock in a Product Engineer interview

A Product Engineer interview is rarely about proving you can name every distributed-systems pattern.
It is about showing you can **start from a user and business problem, make sound trade-offs, build
something that works, and be honest about its limits.** Use this page to structure that story.

> Rule for yourself: **never quote a performance number you have not measured.** The load and chaos
> tests exist, but their results are only in [load-test.md](load-test.md) and the
> [chaos results log](chaos-test.md#results-log) after you run them. Until then say "I built the test
> and here is exactly what it asserts", not "it handles N requests per second".

## Contents

1. [The 30-second pitch](#1-the-30-second-pitch)
2. [The 3-minute walkthrough](#2-the-3-minute-walkthrough)
3. [Start from the product problem](#3-start-from-the-product-problem)
4. [The five talking points that matter](#4-the-five-talking-points-that-matter)
5. [Whiteboard script](#5-whiteboard-script)
6. [Likely questions and good answers](#6-likely-questions-and-good-answers)
7. [Trade-offs and honesty](#7-trade-offs-and-honesty)
8. [Live demo script](#8-live-demo-script)
9. [Product-engineering angles](#9-product-engineering-angles)
10. [Things not to claim](#10-things-not-to-claim)

---

## 1. The 30-second pitch

> "SeatLock is a flash-sale ticketing platform: thousands of people hit a limited-inventory event at
> the same instant, and the one rule is never to sell a ticket twice. A reservation atomically
> decrements stock in Postgres and creates a time-limited hold; four services (inventory, orders,
> payments, notifications) then complete the purchase by exchanging events through Kafka, using a
> transactional outbox so events are never lost and idempotent consumers so duplicates are harmless.
> I wrote the load test that asserts exactly N winners for N tickets, and a chaos script that kills a
> service mid-purchase and checks nothing is lost or duplicated."

## 2. The 3-minute walkthrough

1. **Problem (20 s).** Flash sale, tiny stock, huge burst. Overselling is the unforgivable failure;
   a slow page is just annoying.
2. **Reserve, don't sell (30 s).** A reservation is a _hold_ with a 5-minute deadline. Stock is
   deducted immediately by one atomic `UPDATE ... WHERE available >= q`. Payment then confirms the
   hold or the hold expires and stock returns. Buyers get a fair checkout window; the system never
   needs to "un-sell".
3. **Four services, no direct calls (40 s).** Inventory, order, payment, notification, each with its
   own database. They only talk through Kafka events. A service being down never breaks another
   service's API.
4. **Making events reliable (50 s).** The dual-write problem: you cannot atomically update Postgres and
   publish to Kafka. So each service writes the event to an outbox table in the same transaction as the
   state change, and a relay publishes it. That gives at-least-once delivery, which means every consumer
   must be idempotent: it records the message id in the same transaction as its work.
5. **Failure handling (30 s).** Retries go to a retry topic so one bad message cannot block a
   partition; permanent failures go to a dead-letter queue, and I wrote a CLI to inspect and replay
   them.
6. **Proof and honesty (20 s).** Integration tests on real Postgres for the concurrency claims, a k6
   test, a chaos script. And a written list of known gaps (no refund flow, no auth yet).

## 3. Start from the product problem

Frame decisions by the user experience, not the technology:

| User moment                         | Product requirement                                                  | Technical answer                                                                           |
| ----------------------------------- | -------------------------------------------------------------------- | ------------------------------------------------------------------------------------------ |
| "I clicked Buy at 12:00:00"         | Be fair and fast; tell me clearly if it is gone                      | Atomic stock decrement; `409 SOLD_OUT` is a normal, friendly outcome                       |
| "My phone froze and I tapped twice" | Never charge or reserve twice                                        | `Idempotency-Key` on reserve and pay; replay the stored response                           |
| "I am checking out"                 | Show how long I have; do not lose my seats while I type card details | Hold with `expiresAt`; countdown from `secondsRemaining`; `GET /holds/:id` not cached      |
| "It is sold out"                    | Give me a second chance                                              | Waitlist: released stock is offered to the line before anyone else                         |
| "I paid, where is my order?"        | Tell me the truth even if the system is busy                         | Eventual consistency made visible: order appears via polling, with a clear timeout message |
| "Payment failed"                    | Get my seats released for others and tell me why                     | `PaymentFailed` → order `FAILED` → hold released → notification                            |

## 4. The five talking points that matter

### 1) "Never oversell" is a database property, not an application one

- One statement: `UPDATE events SET available = available - $q WHERE id = $id AND available >= $q`.
- Why it works: row lock; Postgres **re-checks the `WHERE` against the committed row** after waiting
  (that is why request #101 sees 0 and fails); the check and write cannot be separated.
- Backstop: `CHECK (available >= 0)`.
- Why not Redis `DECR` as the source of truth: then correctness depends on Redis durability and on
  keeping two systems consistent (the dual-write problem again).
- Cost to name: one hot row per event serialises. Next step if it matters: bucketed counters.
- Evidence: integration test, 300 concurrent requests for 100 tickets → exactly 100 succeed.

### 2) The dual-write problem and the transactional outbox

- "You cannot commit to Postgres and publish to Kafka atomically."
- Outbox: event row in the same transaction; relay publishes with `FOR UPDATE SKIP LOCKED`.
- Consequence to state unprompted: **at-least-once**, so duplicates exist → consumers dedupe on
  `eventId` using a `processed_events` primary key **inside the same transaction** as the change
  (marker and effect commit or roll back together).
- Bonus: reservations keep succeeding while Kafka is down.

### 3) Idempotency end to end

- Client key → stored with the response, same transaction.
- Same key + same body = replay; same key + different body = 409.
- Concurrent duplicates: per-key advisory lock so the second request waits and replays instead of
  failing with a unique-violation 500.
- Server side: `UNIQUE(hold_id)` on orders, `UNIQUE(order_id)` on payments: "one order per hold, one
  payment per order" are database facts.

### 4) State machines make races boring

- Hold: `ACTIVE → CONFIRMED | RELEASED | EXPIRED`; order: `PENDING_PAYMENT → CONFIRMED | FAILED | EXPIRED`.
  Terminal states have no exits.
- Races (expiry vs payment, failure released twice) resolve via
  `UPDATE ... WHERE status = 'ACTIVE'` under a row lock: exactly one winner, and stock is restored at
  most once.
- The same definition is shared by every service in `packages/shared`.

### 5) Failure is designed in, and testable

- Retry topic vs in-place retry (head-of-line blocking).
- DLQ with the original bytes intact; replay is safe because of idempotency.
- Chaos: kill order-service mid-sale; reservation still accepted; order appears after restart; exactly
  one order per hold; hold, order and payment agree.
- Health: liveness vs readiness (a DB outage must not cause restart storms).

## 5. Whiteboard script

Draw in this order; each step answers the question the previous one raises.

1. Browser → **inventory-service** → Postgres. Write the guarded `UPDATE`. _"This is the only place
   stock changes."_
2. Add the **hold** row with `expires_at`, and the **outbox** row, all in one box labelled "one
   transaction".
3. Add Kafka. Arrow outbox → relay → `inventory.events`. _"Why not publish directly? Dual write."_
4. Add **order-service** consuming it. Draw the `processed_events` insert inside its transaction.
   _"At-least-once, so it must dedupe."_
5. Add `PaymentRequested` → **payment-service** → `PaymentSucceeded/Failed` → fan out to order and
   inventory (and notification).
6. Draw the two compensations: payment failed → release; hold expired → release + expire order.
7. Add the retry topic and DLQ at the side. Stop there unless asked.

## 6. Likely questions and good answers

**"How do you prevent two people buying the last ticket?"**
One atomic conditional update in Postgres. Concurrent updates queue on the row lock, and when the lock
frees, Postgres re-evaluates the `WHERE` against the new row, so the second buyer matches zero rows
and gets sold out. A `CHECK` constraint is the backstop.

**"Why not use Redis for the counter? It's faster."**
It is faster, but then the source of truth for money-adjacent inventory is a second system I must keep
consistent with Postgres, and a failover can lose a decrement. I use Redis only for read caching and
rate limiting, both of which fail open. If one hot event needed more throughput, I would bucket the
counter in Postgres or put Redis in front as a pre-filter that can only say no.

**"What happens if the service crashes after committing but before publishing?"**
Nothing is lost: the event is in the outbox in the same transaction. The relay publishes it after
restart. The reverse (publish succeeded, mark failed) causes a duplicate, which consumers dedupe by
`eventId`.

**"Is it exactly-once?"**
No: at-least-once delivery with idempotent consumers, which gives exactly-once _effects_. Exactly-once
delivery across Postgres and Kafka is not something you get for free.

**"What if messages arrive out of order?"**
Ordering is only per partition key (concert id for inventory events, order id for orders and
payments). Beyond that, handlers are order-tolerant: state machines reject illegal transitions, so a
stale or late event becomes a logged no-op rather than corruption. A retried message may legitimately
arrive after later ones.

**"What if payment succeeds after the hold expired?"**
The order is already `EXPIRED`, a terminal state, so order-service logs "refund required" and ignores
it, and inventory dead-letters the confirmation. There is no automatic refund yet: it is the biggest
known gap, and it is documented and reproducible (chaos test, section 8). The fix is a compensating
refund step in the saga.

**"Orchestration or choreography, and why?"**
Choreography: four services, a short linear flow, and I wanted independent availability and no
orchestrator to run. The price is that the flow is implicit, so I documented it with a diagram and an
event catalog. If the flow grew branches or long timers, I would switch to an orchestrator.

**"Why microservices for this? Isn't that overkill?"**
Honestly, yes: a modular monolith with local transactions would be simpler. I split it to build and
practise the patterns that appear when you do split: outbox, idempotency, saga compensation, DLQ,
chaos testing. In production I would split only along team or scaling boundaries.

**"How did you test it?"**
Unit tests for state machines and handlers; integration tests on real Postgres (Testcontainers) for
the concurrency claims; a k6 stampede that asserts exactly 100 winners for 100 tickets; and a chaos
script that kills a service mid-flow. _(If asked for numbers and you have not run them: say so.)_

**"How would you monitor it?"**
Today: structured logs with a correlation id, health endpoints, Kafka UI. What I would add first:
outbox backlog, consumer lag and DLQ depth as metrics with alerts, because those three tell you the
saga is stuck before users do.

**"How would you scale it?"**
Services are stateless apart from their databases: add instances (outbox claims use `SKIP LOCKED`;
consumers scale with partitions). The real ceiling is the hot row per event; the answer is bucketed
inventory or a waiting room, chosen after measuring.

**"What would you do differently?"**
Unify the messaging conventions in a shared library (the four services have slightly different DLQ
headers), add real auth, build the refund path, and add metrics. Those are listed in
[tradeoffs.md](tradeoffs.md).

**"A message is stuck in the DLQ. What do you do?"**
`npm run dlq:inspect` shows the reason, service and payload. Fix the cause (a bug or a dependency), dry-run
`npm run dlq:replay -- --dry-run --all`, then replay. It is safe because consumers dedupe on `eventId`.
I would not replay garbage or business-rule rejections, because they would fail again.

## 7. Trade-offs and honesty

Interviewers trust candidates who volunteer limits. Offer these before being asked (full list in
[tradeoffs.md](tradeoffs.md)):

- One hot row per event; measure before optimising.
- At-least-once delivery means a permanent idempotency obligation.
- Choreography hides the flow; documentation is the mitigation.
- No refund flow, no authentication, mock payment provider.
- Load and chaos results: tests exist, numbers not yet recorded (or recorded: cite them).
- A real team would likely start as a modular monolith.

## 8. Live demo script

Five minutes, no surprises. Prepare: `docker compose up -d`, `npm run build:shared`, `npm run migrate`,
`npm run seed`, `npm run setup:topics`, `npm run dev`.

1. **Kafka UI** (http://localhost:8080): show the topics, to anchor the architecture.
2. **Reserve in the web app** (or `curl`): show the hold and its countdown.
3. **Order appears:** point out it was created asynchronously via Kafka.
4. **Pay:** order becomes `CONFIRMED` (or `FAILED` about 20% of the time: _"that is the mock's configured
   failure rate, watch the hold release and stock go back"_).
5. **Double-click safety:** repeat the reservation `curl` with the same `Idempotency-Key`: same hold,
   `Idempotent-Replayed: true`.
6. **Chaos (if time):** `npm run chaos -- --dry-run` to show the plan, or run it live with
   `ORDER_SERVICE_PID` and `CHAOS_AUTO_RESTART` set.
7. **DLQ (if time):** push a garbage message (see [chaos-test.md, scenario 6](chaos-test.md#7-scenario-6-poison-message)),
   then `npm run dlq:inspect`.

## 9. Product-engineering angles

Say these in your own words; they are what separate a Product Engineer answer from a pure backend one.

- **Sold-out is a feature, not an error.** The API returns a clear `409 SOLD_OUT`, the UI shows a
  friendly message, and the waitlist turns a dead end into a second chance.
- **Be honest about time.** Holds expose `expiresAt` and `secondsRemaining` computed from the database
  clock so the UI countdown agrees with the server.
- **Eventual consistency is a UX problem as much as a backend one.** Polling with a clear timeout
  message and no scary errors; the reservation already succeeded.
- **Idempotency is a customer-trust feature:** double-taps and retries never double-charge.
- **Instrument what the business cares about:** conversion of holds to orders, expiry rate, sold-out
  rate, time-to-sell-out. (Not built; say what you would measure.)
- **Rate limiting is a product decision:** it protects fairness from bots but also hurts people behind a
  shared IP; choose limits with data.
- **Scope with intent.** Mock payments, no auth: deliberate cuts so the hard parts (inventory
  correctness, reliability) got the effort. Be ready to say what you would build next and why.

## 10. Things not to claim

- Any throughput or latency figure you have not measured. Cite `docs/load-test.md` only after filling it in.
- "Exactly-once delivery." It is at-least-once with idempotent effects.
- "Production ready." It has no auth, refunds, metrics or HA.
- "Verified chaos behaviour" before you have run `npm run chaos` and logged the result.
- That Redis protects against overselling. It does not; Postgres does.
