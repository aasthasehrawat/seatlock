# scripts/

Operational tooling for SeatLock. Everything runs from the **repo root** with `tsx` (already a root
dev dependency) and talks to the local Docker stack.

| Script                              | npm command                                  | What it does                                                             |
| ----------------------------------- | -------------------------------------------- | ------------------------------------------------------------------------ |
| `setup-topics.ts`                   | `npm run setup:topics`                       | Creates the 7 Kafka topics with explicit partition counts. Idempotent.   |
| `dlq-replay.ts`                     | `npm run dlq:inspect` / `npm run dlq:replay` | Lists dead-lettered messages; republishes them to their original topic.  |
| `chaos-test.ts`                     | `npm run chaos`                              | Kills order-service mid-flow and verifies nothing is lost or duplicated. |
| `../tests/load/reservation-load.js` | `npm run load-test`                          | k6 stampede: 1000 VUs vs. a 100-ticket event. Needs k6 installed.        |

## Note on root `package.json` (what is wired, what to add)

The root `package.json` was **not rewritten**. Its scripts block already contained these entries
(they point at the files in this folder), so nothing needs to change for them:

```json
"dlq:inspect": "tsx scripts/dlq-replay.ts inspect",
"dlq:replay":  "tsx scripts/dlq-replay.ts replay",
"load-test":   "k6 run tests/load/reservation-load.js",
"chaos":       "tsx scripts/chaos-test.ts"
```

One script was missing and the one-command startup in the README depends on it, so a single line
was added (and nothing else touched):

```json
"setup:topics": "tsx scripts/setup-topics.ts"
```

If you ever regenerate `package.json`, make sure those five entries survive.

Pass flags through npm with `--`:

```bash
npm run dlq:inspect -- --limit 0 --json
npm run dlq:replay  -- --dry-run --all
npm run chaos       -- --dry-run
npm run setup:topics -- --list
```

## Running without a build

The scripts deliberately do **not** import `@seatlock/shared` (topic names are duplicated as
constants), so they work on a fresh clone before `npm run build:shared`.

They do rely on `kafkajs` being resolvable from the repo root. It is a dependency of the services
and npm workspaces hoist it to the root `node_modules` on `npm install`. If you ever see
`Cannot find module 'kafkajs'`, run `npm install` at the root again; if your npm layout does not
hoist it, add it explicitly with `npm install --save-dev kafkajs` at the root.

## Windows / PowerShell notes

- Setting an env var for one command: `$env:KAFKA_BROKERS = 'localhost:9092'; npm run dlq:inspect`
- `curl` in PowerShell is an alias for `Invoke-WebRequest`. The chaos script prints
  `Invoke-RestMethod` commands on Windows for that reason (and `curl` on macOS/Linux).
- `cp .env.example .env` is `Copy-Item .env.example .env` in PowerShell.

## DLQ message formats (why the replay tool is defensive)

Each service wrote its own dead-letter headers, so `dlq-replay.ts` understands all of them:

| Service              | Original topic header | Other headers written                                                                                       |
| -------------------- | --------------------- | ----------------------------------------------------------------------------------------------------------- |
| order-service        | `x-original-topic`    | `x-original-partition`, `x-original-offset`, `x-failed-service`, `x-failed-at`, `x-error`, business headers |
| inventory-service    | `dlq-original-topic`  | `dlq-reason`, `dlq-error`, `dlq-consumer`, `dlq-attempts`, `dlq-failed-at`                                  |
| payment-service      | `sourceTopic`         | `reason`, `service`, `failedAt`                                                                             |
| notification-service | `sourceTopic`         | `reason`, `service`, `failedAt`                                                                             |

Target-topic resolution order: `--topic` flag, then `x-original-topic`, `dlq-original-topic`,
`sourceTopic`, then the event type (`event-type` / `eventType` / `type` header, or the `type`
field of the envelope). Only `inventory.events`, `orders.events`, `payments.events` and
`notifications.events` are valid targets; `retry.*` and `dlq` are refused so a replay can never loop.

> The inconsistent header names are a known wart (see `docs/tradeoffs.md`, "Known gaps"). A shared
> `publishToDlq()` helper in `@seatlock/shared` would remove the need for this translation layer.

## Load-test prerequisites (short version, full detail in `docs/load-test.md`)

```powershell
# 1. inventory-service must NOT rate-limit the single k6 client IP (default is 20 requests / 60 s):
$env:RATE_LIMIT_MAX = '1000000'; npm run dev -w @seatlock/inventory-service
# 2. full stock on the 100-ticket event, then wait >5 s for the read cache:
npm run seed -w @seatlock/inventory-service -- --reset
# 3. run
npm run load-test
```
