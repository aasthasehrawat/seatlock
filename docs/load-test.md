# Load test: flash-sale reservation stampede

> **Status: Not yet executed on this machine.**
>
> Docker Desktop 4.97 is installed, but the engine cannot start because no WSL2 Linux distro is
> present (`wsl -l -v` reports none). Install Ubuntu from the Microsoft Store (or
> `wsl --install -d Ubuntu`), reboot if prompted, wait until `docker info` shows a Server Version,
> then run the steps below and fill every `TBD` cell with real output. Do not invent numbers.

## What this test proves

One event, **100 tickets**, **1000 virtual users** all calling `POST /reservations` at the same
moment, each with a unique `Idempotency-Key` and a unique `userId`.

The claim under test is a _correctness_ claim, not a speed claim:

| Property             | Expected outcome                                                    |
| -------------------- | ------------------------------------------------------------------- |
| No overselling       | exactly **100** responses are `201`                                 |
| Fair rejection       | exactly **900** responses are `409 SOLD_OUT`                        |
| No collateral damage | **0** responses of any other kind (no `5xx`, no `429`, no timeouts) |
| Stock accounting     | `events.available` ends at exactly **0**, never negative            |
| Idempotent replay    | re-sending a winning request returns the same hold, buys nothing    |

Why this holds is explained in [ADR-002](adr/002-inventory-concurrency.md): the decrement is a single
`UPDATE events SET available = available - $qty WHERE id = $id AND available >= $qty`, backed by a
`CHECK (available >= 0)` constraint.

The k6 thresholds in [`tests/load/reservation-load.js`](../tests/load/reservation-load.js) encode
exactly these expectations, so a run either passes them or the process exits non-zero.

## Prerequisites

1. **Tools:** Docker, Node 20+, and [k6](https://grafana.com/docs/k6/latest/set-up/install-k6/)
   (`winget install k6`, `brew install k6`, or the Docker image `grafana/k6`).
2. **Infrastructure and services running:**

   ```bash
   docker compose up -d
   npm install && npm run build:shared
   npm run migrate && npm run setup:topics
   ```

3. **Raise the per-IP rate limit before starting inventory-service.** This is the step people miss.
   `POST /reservations` is limited per client IP (Redis, fixed window). Defaults:
   `RATE_LIMIT_MAX=20` per `RATE_LIMIT_WINDOW_SECONDS=60`. k6 sends all 1000 requests from one IP,
   so with defaults roughly 980 of them would receive `429 RATE_LIMITED` and the test would
   correctly fail. For the load test only:

   ```powershell
   # PowerShell
   $env:RATE_LIMIT_MAX = '1000000'
   npm run dev:services
   ```

   ```bash
   # bash / zsh
   RATE_LIMIT_MAX=1000000 npm run dev:services
   ```

   Only `inventory-service` reads this variable, and it does **not** load `.env` by itself, so it
   must be present in the environment of the shell that launches the service. Restore the default
   afterwards: the limiter is a deliberate protection against bots during a real sale.

4. **Seed full stock and clear stale state.** The seed creates event
   `00000000-0000-4000-8000-00000000a002` with exactly 100 tickets:

   ```bash
   npm run seed -w @seatlock/inventory-service -- --reset
   docker exec seatlock-redis redis-cli FLUSHDB     # clears rate-limit counters and the read cache
   ```

   `--reset` truncates inventory's `holds`, `outbox`, `idempotency_keys`, `waitlist` and
   `processed_events` tables and restores stock. It does **not** touch order/payment/notification
   databases, so orders from earlier runs remain there (harmless for this test).

   `GET /events/:id` is cached for 5 s. The k6 `setup()` aborts with a clear message if the event
   does not currently show exactly 100 available; if you just reseeded, wait 6 s (or `FLUSHDB`).

## Run it

```bash
npm run load-test
# equivalent: k6 run tests/load/reservation-load.js
```

Common variations (pass with `-e`):

```bash
k6 run -e VUS=200 -e TICKETS=100 tests/load/reservation-load.js          # smaller stampede
k6 run -e BASE_URL=http://localhost:3001 tests/load/reservation-load.js   # explicit target
k6 run -e REPLAY_CHECK=false tests/load/reservation-load.js               # skip idempotent-replay probes
k6 run -e P95_MS=1500 tests/load/reservation-load.js                      # OPT-IN latency gate (your number)
k6 run -e QUANTITY=2 tests/load/reservation-load.js                       # 2 tickets each -> 50 winners
```

Between runs always repeat step 4 (reseed + `FLUSHDB`), otherwise `setup()` will refuse to start
because the event is already sold out.

The run takes a few seconds plus a ~6.5 s wait in teardown: the script waits out the 5 s read cache so
it can read the authoritative final stock.

### Where output goes

- **stdout:** a paste-ready summary (outcomes, latency percentiles, thresholds, verdict).
- **`docs/load-test-raw.json`:** the full k6 summary JSON (git-ignored).

## Verify independently in Postgres

The k6 verdict is about what clients saw. Confirm the database agrees:

```bash
docker exec seatlock-postgres-inventory psql -U seatlock -d inventory -c \
  "SELECT available, total_tickets FROM events WHERE id = '00000000-0000-4000-8000-00000000a002';"

docker exec seatlock-postgres-inventory psql -U seatlock -d inventory -c \
  "SELECT status, COUNT(*) AS holds, SUM(quantity) AS tickets FROM holds
    WHERE event_id = '00000000-0000-4000-8000-00000000a002' GROUP BY status;"
```

Expected: `available = 0`, and holds `ACTIVE` count = 100 (hold TTL is 300 s, so they have not expired
yet if you check promptly). The invariant from `hold-lifecycle.ts` is
`available + Σ(ACTIVE) + Σ(CONFIRMED) = total_tickets`.

Then let the system drain and check the downstream effects (optional, shows the saga under load):

```bash
# 100 orders should appear in order-service (eventually); never more than one per hold.
docker exec seatlock-postgres-orders psql -U seatlock -d orders -c \
  "SELECT status, COUNT(*) FROM orders GROUP BY status;"
docker exec seatlock-postgres-orders psql -U seatlock -d orders -c \
  "SELECT hold_id, COUNT(*) FROM orders GROUP BY hold_id HAVING COUNT(*) > 1;"   # must return 0 rows
```

## Results

### Run metadata

| Field                          | Value                                                |
| ------------------------------ | ---------------------------------------------------- |
| Date / time                    | TBD                                                  |
| Git commit                     | TBD                                                  |
| Machine (CPU / RAM / OS)       | TBD                                                  |
| Docker resource limits         | TBD                                                  |
| k6 version (`k6 version`)      | TBD                                                  |
| Node version                   | TBD                                                  |
| `VUS` / `TICKETS` / `QUANTITY` | TBD                                                  |
| `RATE_LIMIT_MAX` used          | TBD                                                  |
| `DB_POOL_MAX` (inventory)      | TBD (default 20)                                     |
| Everything on one machine?     | TBD (load generator shares CPU with services if yes) |

### Correctness

| Metric                        | Expected | Actual |
| ----------------------------- | -------- | ------ |
| `reservations_attempted`      | 1000     | TBD    |
| `reservations_success` (201)  | 100      | TBD    |
| `reservations_sold_out` (409) | 900      | TBD    |
| `reservations_rate_limited`   | 0        | TBD    |
| `reservations_unexpected`     | 0        | TBD    |
| `reservation_replay_mismatch` | 0        | TBD    |
| final stock (`available`)     | 0        | TBD    |
| k6 thresholds                 | all pass | TBD    |

### Performance (`POST /reservations`)

| Metric             | success (201) | sold out (409) | all requests |
| ------------------ | ------------- | -------------- | ------------ |
| avg                | TBD           | TBD            | TBD          |
| median             | TBD           | TBD            | TBD          |
| p90                | TBD           | TBD            | TBD          |
| p95                | TBD           | TBD            | TBD          |
| p99                | TBD           | TBD            | TBD          |
| max                | TBD           | TBD            | TBD          |
| throughput (req/s) | n/a           | n/a            | TBD          |

### Raw k6 output

Paste the stdout summary here, unedited:

```text
TBD (paste the "=== SeatLock reservation load test: results ===" block)
```

## Analysis (fill in after the run)

Keep each section short and tied to evidence from the run (a metric, a log line, a query result).
If a section does not apply, say why instead of deleting it.

### 1. Did the correctness claim hold?

- Successes vs. tickets: TBD
- Any oversell, negative stock, or duplicate hold per idempotency key: TBD
- Postgres cross-check result: TBD

### 2. Latency shape

- Is there a visible difference between winners (201) and losers (409)? Winners do more work (hold
  insert, outbox insert, idempotency record); losers do the failed `UPDATE` plus one existence
  check. TBD
- Tail behaviour (p99 and max versus median): TBD
- Hypothesis for the tail, if any (row-lock queueing on the single event row, connection-pool
  wait, GC, load generator saturation): TBD

### 3. Where is the bottleneck?

Candidates specific to this design; confirm or rule out each with evidence:

| Candidate                                                                           | How to check                                                                                                                              | Finding |
| ----------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------- | ------- |
| Row-lock serialisation on the one `events` row (by design, see ADR-002)             | `SELECT wait_event_type, wait_event, count(*) FROM pg_stat_activity GROUP BY 1,2` during the run; look for `Lock` / `transactionid` waits | TBD     |
| Postgres connection pool (`DB_POOL_MAX`, default 20; `connectionTimeoutMillis` 5 s) | Any `unexpected` 500s with pool-timeout errors in the inventory log?                                                                      | TBD     |
| `statement_timeout` (10 s) on the hot row                                           | Any `canceling statement due to statement timeout` errors?                                                                                | TBD     |
| Node event loop (single process)                                                    | CPU of the inventory-service process                                                                                                      | TBD     |
| Load generator itself (k6 + services on one box)                                    | k6 CPU; compare with a smaller `VUS` run                                                                                                  | TBD     |
| Outbox relay / Kafka                                                                | Relay backlog: `SELECT count(*) FROM outbox WHERE published = false` over time                                                            | TBD     |

### 4. Downstream effects

- Time for all 100 `InventoryReserved` events to become 100 orders: TBD
- Outbox backlog peak and drain time: TBD
- Any messages in the DLQ afterwards (`npm run dlq:inspect`)? TBD

### 5. Limits of this test (be honest about them)

- Single machine, single inventory-service instance, single Postgres: it says nothing about
  horizontal scale.
- One hot event: it is the worst case for lock contention. Many events at once would behave
  differently. TBD whether tested.
- The rate limiter was raised or disabled for the run, so this does not test the limiter.
- `quantity = 1` unless overridden.
- Redis read cache not under test (only `GET /events/:id` in setup/teardown).

### 6. Conclusions and follow-ups

- Conclusion in one sentence: TBD
- Changes this suggests (pool size, bucketed counters, scale-out): TBD
- Next experiment: TBD

## Reproducing the exact run later

Record the commit, the exact command line (including every `-e` flag), the environment variables of
inventory-service, and `docker compose ps` output. A load-test result without those is an anecdote.
