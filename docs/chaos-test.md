# Chaos test procedure

Manual, reproducible failure-injection scenarios for SeatLock. The headline scenario (order-service
crash mid-flow) is automated by `npm run chaos`; this document is the full procedure behind it, plus
further scenarios you can run by hand.

> **Honesty note.** The "Expected" results below are derived from reading the code (outbox, consumer
> groups, idempotency, state machines) and from the unit/integration tests. They have **not** all been
> observed on a running stack yet. Record what you actually see in the
> [results log](#results-log) at the bottom; a mismatch is a finding, not a failure of the document.

## Contents

1. [Setup](#1-setup)
2. [Scenario 1: order-service crashes during a sale](#2-scenario-1-order-service-crashes-during-a-sale) (automated by `npm run chaos`)
3. [Scenario 2: payment-service is down when the customer pays](#3-scenario-2-payment-service-is-down-when-the-customer-pays)
4. [Scenario 3: Kafka goes down](#4-scenario-3-kafka-goes-down)
5. [Scenario 4: Redis goes down](#5-scenario-4-redis-goes-down)
6. [Scenario 5: inventory database goes down, retries exhaust, DLQ and replay](#6-scenario-5-inventory-database-goes-down-retries-exhaust-dlq-and-replay)
7. [Scenario 6: poison message](#7-scenario-6-poison-message)
8. [Variation: outage longer than the hold TTL](#8-variation-outage-longer-than-the-hold-ttl)
9. [If a check fails](#9-if-a-check-fails)
10. [Results log](#results-log)

---

## 1. Setup

```bash
docker compose up -d
npm install
npm run build:shared
npm run migrate
npm run seed
npm run setup:topics
npm run dev:services          # leave running in terminal 1 (API only; no web app needed)
```

Use terminal 2 for the commands below. Event `00000000-0000-4000-8000-00000000a003` ("Acoustic
Sundown Sessions", 40 tickets) is used so the 100-ticket load-test event stays untouched.

Variables used throughout:

```bash
# bash / zsh
EVENT=00000000-0000-4000-8000-00000000a003
USER_ID=$(uuidgen)          # or: python -c "import uuid;print(uuid.uuid4())"
```

```powershell
# PowerShell
$EVENT  = '00000000-0000-4000-8000-00000000a003'
$USER   = [guid]::NewGuid().ToString()
```

Handy inspection commands:

| What                     | Command                                                                                                                                                      |
| ------------------------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| Consumer lag (order)     | `docker exec seatlock-kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group order-service`                      |
| Consumer lag (inventory) | same, `--group inventory-service`                                                                                                                            |
| Consumer lag (payment)   | same, `--group payment-service`                                                                                                                              |
| Orders in Postgres       | `docker exec seatlock-postgres-orders psql -U seatlock -d orders -c "SELECT id, hold_id, status, payment_id FROM orders ORDER BY created_at DESC LIMIT 10;"` |
| Inventory outbox backlog | `docker exec seatlock-postgres-inventory psql -U seatlock -d inventory -c "SELECT count(*) FROM outbox WHERE published = false;"`                            |
| Order outbox backlog     | `docker exec seatlock-postgres-orders psql -U seatlock -d orders -c "SELECT count(*) FROM outbox WHERE published = false;"`                                  |
| Holds by status          | `docker exec seatlock-postgres-inventory psql -U seatlock -d inventory -c "SELECT status, count(*) FROM holds GROUP BY status;"`                             |
| Stock                    | `docker exec seatlock-postgres-inventory psql -U seatlock -d inventory -c "SELECT id, available, total_tickets FROM events;"`                                |
| Dead letters             | `npm run dlq:inspect`                                                                                                                                        |
| Browse topics            | Kafka UI at http://localhost:8080                                                                                                                            |
| Find a service PID       | PowerShell: `(Get-NetTCPConnection -LocalPort 3002 -State Listen).OwningProcess` / bash: `lsof -ti :3002 -sTCP:LISTEN`                                       |

> **Inventory read cache:** `GET /events` and `GET /events/:id` are cached for 5 s. When comparing
> `available` before and after, wait 6 s, or read the database directly.

Health endpoints (all four services): `GET /health/live` (process up) and `GET /health/ready`
(dependencies reachable; `503` when not).

---

## 2. Scenario 1: order-service crashes during a sale

**Hypothesis.** Reservations do not depend on order-service. While it is down, `InventoryReserved`
events wait in Kafka. After restart the consumer group resumes from its committed offset, creates the
order, and no hold ever has zero or two orders.

### Automated

```bash
npm run chaos -- --dry-run     # prints every step, exact command and expected result; calls nothing
npm run chaos                  # runs it; polls for you to stop / restart order-service
```

Fully automatic (the script kills and restarts order-service itself):

```powershell
$env:ORDER_SERVICE_PID = (Get-NetTCPConnection -LocalPort 3002 -State Listen).OwningProcess
$env:CHAOS_AUTO_RESTART = 'true'
npm run chaos
```

```bash
ORDER_SERVICE_PID=$(lsof -ti :3002 -sTCP:LISTEN) CHAOS_AUTO_RESTART=true npm run chaos
```

The script ends with `RESULT: PASS` / `RESULT: FAIL` and a list of failed checks.

### Manual procedure (what the script does)

**Step 1: all services healthy**

```bash
for p in 3001 3002 3003 3004; do curl -s -o /dev/null -w "$p %{http_code}\n" http://localhost:$p/health/ready; done
```

```powershell
3001..3004 | ForEach-Object { "{0} {1}" -f $_, (Invoke-WebRequest -UseBasicParsing "http://localhost:$_/health/ready").StatusCode }
```

Expected: `200` four times.

**Step 2: list events**

```bash
curl -s http://localhost:3001/events
```

```powershell
(Invoke-RestMethod http://localhost:3001/events).events | Format-Table name, available, totalTickets
```

Expected: three seeded events. There is no create-event endpoint; events come from
`npm run seed -w @seatlock/inventory-service -- --reset`. Note `available` for the chaos event.

**Step 3: reservation A, before the outage (control)**

```bash
curl -s -i -X POST http://localhost:3001/reservations \
  -H 'Content-Type: application/json' -H 'Idempotency-Key: chaos-manual-A' \
  -d "{\"eventId\":\"$EVENT\",\"userId\":\"$USER_ID\",\"quantity\":1}"
```

```powershell
$h = @{ 'Idempotency-Key' = 'chaos-manual-A' }
$body = @{ eventId = $EVENT; userId = $USER; quantity = 1 } | ConvertTo-Json
$a = Invoke-RestMethod -Method Post -Uri http://localhost:3001/reservations -Headers $h -ContentType 'application/json' -Body $body
$a
```

Expected: `201`, body with `holdId`, `status: "ACTIVE"`, `expiresAt` about 5 minutes ahead.
Then, within 1 to 3 seconds:

```bash
curl -s "http://localhost:3002/orders?userId=$USER_ID"
```

Expected: one order, `holdId` = the hold above, `status: "PENDING_PAYMENT"`.

**Step 4: stop order-service**

Find its PID and kill it hard (a crash, not a graceful shutdown):

```powershell
Stop-Process -Id (Get-NetTCPConnection -LocalPort 3002 -State Listen).OwningProcess -Force
```

```bash
kill -9 "$(lsof -ti :3002 -sTCP:LISTEN)"
```

Do **not** press Ctrl+C in the `npm run dev:services` terminal: that stops all four services.
Confirm it is down:

```bash
curl -s -o /dev/null -w "%{http_code}\n" http://localhost:3002/health/live    # connection refused
```

**Step 5: reservation B during the outage, plus an idempotent retry**

```bash
USER_B=$(uuidgen)
curl -s -i -X POST http://localhost:3001/reservations \
  -H 'Content-Type: application/json' -H 'Idempotency-Key: chaos-manual-B' \
  -d "{\"eventId\":\"$EVENT\",\"userId\":\"$USER_B\",\"quantity\":1}"
# run the exact same command a second time
```

Expected:

- first call `201` (inventory never calls order-service);
- second call `201` with header `Idempotent-Replayed: true` and the **same** `holdId`; stock is
  decremented once, not twice.

Wait about 20 seconds, then look at the backlog:

```bash
docker exec seatlock-kafka /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 --describe --group order-service
```

Expected: `LAG` greater than 0 on `inventory.events`: the `InventoryReserved` event is waiting. In
the inventory database the outbox row is already marked `published = true`: the message is safely in
Kafka; it just has no consumer right now.

**Step 6: restart order-service**

```bash
npm run dev -w @seatlock/order-service
```

It applies migrations (idempotent), connects the producer and both consumers, then starts listening.
Poll until ready:

```bash
curl -s http://localhost:3002/health/ready      # {"status":"ready","checks":{"postgres":"up","kafka-producer":"up","kafka-consumer":"up","outbox-relay":"up"}}
```

**Step 7: verify**

```bash
curl -s "http://localhost:3002/orders?userId=$USER_B"
curl -s "http://localhost:3001/holds/<holdId of B>"
docker exec seatlock-postgres-orders psql -U seatlock -d orders -c \
  "SELECT hold_id, COUNT(*) FROM orders GROUP BY hold_id HAVING COUNT(*) > 1;"
```

Expected:

| Check                                     | Expected                                                   |
| ----------------------------------------- | ---------------------------------------------------------- |
| Order for B                               | appears within seconds of the service being ready          |
| Order status / amount                     | `PENDING_PAYMENT`, `amountCents` = unit price x quantity   |
| Orders per hold                           | exactly 1, also 10 s later (no late duplicates)            |
| Duplicate-hold SQL                        | `(0 rows)`                                                 |
| `GET /holds/<B>`                          | `ACTIVE`, `secondsRemaining` > 0 (outage shorter than TTL) |
| `GET /events/:id` `available` (after 6 s) | before minus 2                                             |

**Step 7b: pay both orders, with idempotency checks**

```bash
ORDER_B=<order id>
curl -s -i -X POST "http://localhost:3002/orders/$ORDER_B/pay" -H 'Idempotency-Key: chaos-manual-B-pay' \
  -H 'Content-Type: application/json' -d '{}'
# same command again -> 202 + Idempotent-Replayed: true + same paymentId
# different key      -> 409
curl -s -i -X POST "http://localhost:3002/orders/$ORDER_B/pay" -H 'Idempotency-Key: chaos-manual-B-pay-2' \
  -H 'Content-Type: application/json' -d '{}'
```

Expected: `202` with `paymentId` and `paymentStatus: "REQUESTED"`; replay `202` with
`Idempotent-Replayed: true`; a different key gets `409` (`PAYMENT_ALREADY_INITIATED`, or
`ORDER_NOT_PAYABLE` if the payment already settled). After about a second:

```bash
curl -s "http://localhost:3002/orders/$ORDER_B"                 # CONFIRMED (~80%) or FAILED (~20%)
curl -s "http://localhost:3003/payments?orderId=$ORDER_B"       # exactly one payment
curl -s "http://localhost:3001/holds/<holdId of B>"             # CONFIRMED if order CONFIRMED, RELEASED if FAILED
```

The payment-service simulates a 20% failure rate (`PAYMENT_FAILURE_RATE`) with 500 ms latency
(`PAYMENT_LATENCY_MS`), so either terminal outcome is correct. What matters is that all three services
agree. If it FAILED, stock goes back up by one (inventory releases the hold).

### Pass criteria

- Reservation accepted during the outage; retry replayed.
- Order created exactly once after restart; no manual repair.
- Hold, order and payment end in agreeing states; stock arithmetic holds.

---

## 3. Scenario 2: payment-service is down when the customer pays

**Hypothesis.** `POST /orders/:id/pay` returns `202` regardless, because it only writes
`PaymentRequested` to order-service's outbox. The command waits on `payments.events`. When
payment-service returns it processes the command exactly once.

```powershell
# 1. reserve + wait for the order (as in Scenario 1, steps 3), then:
Stop-Process -Id (Get-NetTCPConnection -LocalPort 3003 -State Listen).OwningProcess -Force
# 2. pay
curl.exe -s -i -X POST "http://localhost:3002/orders/$ORDER/pay" -H "Idempotency-Key: chaos-p-1" -H "Content-Type: application/json" -d "{}"
# 3. wait 20 s: the order stays PENDING_PAYMENT
curl.exe -s "http://localhost:3002/orders/$ORDER"
# 4. restart
npm run dev -w @seatlock/payment-service
# 5. a few seconds later
curl.exe -s "http://localhost:3002/orders/$ORDER"
curl.exe -s "http://localhost:3003/payments?orderId=$ORDER"
```

Expected: `202` while payment-service is down; order `PENDING_PAYMENT` during the outage; after
restart one payment, order `CONFIRMED` or `FAILED`, hold agrees. The customer's hold is the clock:
keep the outage under `HOLD_TTL_SECONDS` (300 s) or see [section 8](#8-variation-outage-longer-than-the-hold-ttl).

---

## 4. Scenario 3: Kafka goes down

**Hypothesis.** Reservations still succeed (they only need Postgres). Events accumulate in the
inventory outbox. After Kafka returns, the relay drains the backlog and the saga continues.

```bash
docker stop seatlock-kafka
# reserve a ticket (Scenario 1, step 3 command): expect 201
docker exec seatlock-postgres-inventory psql -U seatlock -d inventory -c \
  "SELECT event_type, published FROM outbox ORDER BY created_at DESC LIMIT 5;"
# expect published = f for the newest InventoryReserved row
curl -s -i http://localhost:3002/health/ready      # expect 503 (kafka-producer / kafka-consumer down)
docker start seatlock-kafka
```

Wait for the broker (about 20 to 30 s; `docker compose ps` shows `healthy`), then:

```bash
docker exec seatlock-postgres-inventory psql -U seatlock -d inventory -c \
  "SELECT count(*) FROM outbox WHERE published = false;"        # drains to 0
curl -s "http://localhost:3002/orders?userId=$USER_ID"           # the order appears
```

Expected: no reservation lost; backlog drains by itself; the order appears. Delivery is at-least-once,
so duplicates are possible but consumers dedupe on `eventId`.

> The Kafka container keeps its data on its own filesystem (`/tmp/kraft-combined-logs`, no volume).
> `docker stop` / `docker start` preserves it; `docker compose down` does **not**. If a service stays
> unready more than a minute after Kafka is healthy, restart that service and log it as a finding
> (producer/consumer reconnect behaviour is a real thing to observe, not assume).

---

## 5. Scenario 4: Redis goes down

**Hypothesis.** Redis is an optimisation (read cache and rate limiter), never a source of truth, and
every helper fails open.

```bash
docker stop seatlock-redis
curl -s -i http://localhost:3001/events | head -n 20       # 200, header X-Cache: MISS
curl -s -i http://localhost:3001/health/ready              # 200, checks.redis = "down"
# reserve a ticket: expect 201 (rate limiting is skipped while Redis is down)
docker start seatlock-redis
```

Expected: everything keeps working, reads come from Postgres, readiness stays `200` (Redis is
reported but does not fail readiness). Security trade-off: while Redis is down the per-IP rate limit is
not enforced.

---

## 6. Scenario 5: inventory database goes down, retries exhaust, DLQ and replay

**Hypothesis.** A long enough dependency outage pushes messages through `retry.5s` and finally into
the DLQ with their payload intact. Once the dependency is back, `dlq:replay` re-processes them and the
system converges.

Time-sensitive: finish within `HOLD_TTL_SECONDS` (300 s) of the reservation.

```bash
# 1. reserve and wait for the order to exist (Scenario 1, step 3). Note ORDER and the holdId.
# 2. take away inventory's database
docker stop seatlock-postgres-inventory
# 3. pay (order-service and payment-service do not need the inventory DB)
curl -s -i -X POST "http://localhost:3002/orders/$ORDER/pay" -H 'Idempotency-Key: chaos-db-1' \
  -H 'Content-Type: application/json' -d '{}'
```

If the payment succeeds (~80%; if it fails, the release path is exercised instead; repeat with a new
reservation if you want the confirm path), order-service emits `OrderConfirmed` and payment-service
emitted `PaymentSucceeded`. Inventory consumes both and cannot reach its database:

- each attempt fails after the 5 s pool connection timeout;
- the message is republished to `retry.5s` with attempt metadata, and re-tried about 5 s later;
- after `CONSUMER_MAX_ATTEMPTS` (default 3) total attempts it goes to `dlq` with reason
  `MAX_ATTEMPTS_EXCEEDED`.

Allow roughly 30 to 60 seconds, then:

```bash
npm run dlq:inspect
```

Expected: two entries (`PaymentSucceeded` and `OrderConfirmed`), `failed in: inventory-service`,
`reason: MAX_ATTEMPTS_EXCEEDED`, `would replay to: payments.events` / `orders.events`.

Recover and replay:

```bash
docker start seatlock-postgres-inventory
npm run dlq:replay -- --dry-run --all        # review the plan
npm run dlq:replay -- --all --yes
```

Expected: within seconds `GET /holds/<holdId>` shows `CONFIRMED` with `orderId` set. The other
consumer groups (order-service, notification-service) also see the replayed messages and skip them as
duplicates (same envelope `eventId`). Replaying both messages is safe: the second confirmation is a
no-op because the hold is already confirmed for the same order.

> The DLQ is append-only: replaying does not remove entries. Do not replay the same entries over and
> over; it is harmless but noisy. Replayed copies carry `x-replayed-at` and `x-replayed-from-dlq-*`
> headers.

If you are not fast enough and the hold expires first, the replay will correctly be refused
(`INVALID_STATE_TRANSITION`) and re-dead-lettered; see section 8.

---

## 7. Scenario 6: poison message

**Hypothesis.** Garbage on a shared topic never blocks a partition and never crashes a consumer; each
consuming service parks it in the DLQ.

```bash
echo 'this is not json' | docker exec -i seatlock-kafka /opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server localhost:9092 --topic payments.events
npm run dlq:inspect
```

Expected: up to four DLQ entries, one per consumer group on `payments.events` (order-service,
inventory-service, payment-service, notification-service), each with a different header dialect
(`x-original-topic`, `dlq-original-topic`, `sourceTopic`): this is why `dlq-replay.ts` has a
translation layer. All services stay healthy and keep processing real traffic.

Do **not** replay these: garbage fails again by design. (`npm run dlq:replay -- --dry-run --all` shows
what would happen.)

---

## 8. Variation: outage longer than the hold TTL

A hold is a lease. If order-service or payment-service is down longer than `HOLD_TTL_SECONDS`, the
business reality changes while the saga is stalled.

To demonstrate in seconds instead of minutes, start inventory-service with a short TTL (inventory does
**not** read `.env`, so set it in the shell that starts the service):

```powershell
$env:HOLD_TTL_SECONDS = '30'; npm run dev -w @seatlock/inventory-service
```

Then repeat Scenario 2 (payment-service down) but keep it down for more than 30 s plus the sweep
interval (5 s).

Expected sequence (from the code):

1. Expiry sweeper moves the hold `ACTIVE -> EXPIRED`, returns the stock, emits `InventoryReleased`
   (`EXPIRED`) and `HoldExpired`.
2. order-service moves the order `PENDING_PAYMENT -> EXPIRED` and emits `OrderExpired` (inventory
   ignores it: already expired).
3. payment-service comes back and charges the (mock) payment, emitting `PaymentSucceeded`.
4. order-service sees a success for an `EXPIRED` order: logs `late payment for a terminal order:
refund required` and ignores it. The state machine correctly forbids resurrecting the order.
5. inventory-service tries to confirm an `EXPIRED` hold: `InvalidStateTransition`, which is
   non-retryable, so the message goes to the DLQ (`dlq-reason: INVALID_STATE_TRANSITION`).

**Finding to record:** the customer was charged (in the mock) for tickets they do not have, and
nothing refunds them. This is a known gap (see [tradeoffs](tradeoffs.md)); a real system needs a
compensating refund. Replaying the DLQ entry will not help and is not the right fix.

---

## 9. If a check fails

| Symptom                                     | Where to look                                                                                                                                                              |
| ------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Order never appears after restart           | order-service logs; consumer lag; `docker exec seatlock-postgres-inventory psql ... "SELECT * FROM outbox WHERE published = false"` (stuck outbox?); `npm run dlq:inspect` |
| Order appears but duplicated                | Should be impossible (`UNIQUE(hold_id)`). Capture both rows and the `processed_events` table, it is a real bug                                                             |
| Hold stays `ACTIVE` after order `CONFIRMED` | inventory consumer lag on `orders.events` / `payments.events`; DLQ                                                                                                         |
| Stock off by one                            | Cache: wait 6 s. Otherwise check the invariant `available + SUM(ACTIVE) + SUM(CONFIRMED) = total_tickets`                                                                  |
| `429` when reserving                        | per-IP limiter (20 / 60 s default); wait a minute, or raise `RATE_LIMIT_MAX` for testing                                                                                   |
| Service never becomes ready after restart   | its `/health/ready` body names the failing check (`postgres`, `kafka-producer`, `kafka-consumer`, `outbox-relay`)                                                          |
| Correlate one flow across services          | `X-Correlation-Id` sent on reserve/pay is carried in event envelopes and logs; search all four logs for it                                                                 |

---

## Results log

Copy this table for every run. Leave cells blank until you have actually run the scenario.

| Date | Scenario                      | Commit | Outcome (PASS / FAIL / surprise) | Evidence (command output, log line) | Follow-up |
| ---- | ----------------------------- | ------ | -------------------------------- | ----------------------------------- | --------- |
|      | 1. order-service crash        |        |                                  |                                     |           |
|      | 2. payment-service down       |        |                                  |                                     |           |
|      | 3. Kafka down                 |        |                                  |                                     |           |
|      | 4. Redis down                 |        |                                  |                                     |           |
|      | 5. inventory DB down + replay |        |                                  |                                     |           |
|      | 6. poison message             |        |                                  |                                     |           |
|      | 8. outage > hold TTL          |        |                                  |                                     |           |
