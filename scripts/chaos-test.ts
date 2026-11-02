/**
 * chaos-test.ts: a reproducible, self-checking chaos scenario for the SeatLock saga.
 *
 * QUESTION BEING ANSWERED
 *   "If order-service dies while customers are reserving tickets, do we lose orders, create
 *    duplicate orders, or leave inventory and orders disagreeing once it comes back?"
 *
 * EXPECTED ANSWER (the properties this script verifies)
 *   1. inventory-service keeps accepting reservations while order-service is down. It does not call
 *      order-service; the InventoryReserved event is committed to the inventory outbox and published
 *      to Kafka, where it simply waits (consumer lag grows).
 *   2. When order-service restarts, its consumer group resumes from its last committed offset and
 *      the order appears: eventual consistency, no manual repair.
 *   3. Exactly ONE order exists per hold (UNIQUE(hold_id) + processed_events), even though Kafka is
 *      at-least-once and the outbox relay may re-send.
 *   4. Retrying a reservation / payment request with the same Idempotency-Key replays the stored
 *      response and creates nothing new.
 *   5. After payment settles, hold status and order status agree (CONFIRMED <-> CONFIRMED,
 *      FAILED <-> RELEASED with stock returned), and stock arithmetic adds up.
 *
 * THE SCENARIO (what the script does, in order)
 *   Step 1  Health-check all four services.
 *   Step 2  List events and pick the chaos event (events are seeded; the API has no create-event
 *           endpoint, so "create" means `npm run seed`).
 *   Step 3  BEFORE the outage: reserve ticket A and wait for its order to appear (control group).
 *   Step 4  Stop order-service (kill it automatically if ORDER_SERVICE_PID is set, else instruct).
 *   Step 5  DURING the outage: reserve ticket B, retry it with the same key (idempotency), confirm
 *           inventory still answers, wait, and show consumer lag when Docker is available.
 *   Step 6  Restart order-service (automatically if CHAOS_AUTO_RESTART=true, else instruct) and
 *           wait for it to become ready.
 *   Step 7  Verify: orders for A and B exist exactly once, correct amount, PENDING_PAYMENT; pay
 *           both (with idempotent retries); wait for terminal state; check hold/order/payment
 *           agreement and stock arithmetic.
 *
 * USAGE (from the repo root, with `npm run dev` or `npm run dev:services` running in another terminal)
 *   npm run chaos                         # interactive-ish: polls for you to stop/restart order-service
 *   npm run chaos -- --dry-run            # print every step, command and expected result; touch nothing
 *   npm run chaos -- --outage-seconds 45  # keep order-service down longer (default 20)
 *   npm run chaos -- --skip-payment       # stop after verifying the orders exist exactly once
 *
 *   # Fully automatic (kills and restarts the service for you):
 *   #   PowerShell:  $env:ORDER_SERVICE_PID = (Get-NetTCPConnection -LocalPort 3002 -State Listen).OwningProcess
 *   #                $env:CHAOS_AUTO_RESTART = 'true'; npm run chaos
 *   #   bash:        ORDER_SERVICE_PID=$(lsof -ti :3002 -sTCP:LISTEN) CHAOS_AUTO_RESTART=true npm run chaos
 *
 * ENVIRONMENT
 *   INVENTORY_URL / ORDER_URL / PAYMENT_URL / NOTIFICATION_URL   defaults http://localhost:300{1,2,3,4}
 *   CHAOS_EVENT_ID        concert to reserve (default 00000000-0000-4000-8000-00000000a003, 40 tickets)
 *   ORDER_SERVICE_PID     if set, the script kills this PID (and its children) at step 4
 *   CHAOS_AUTO_RESTART    "true" -> the script restarts order-service itself at step 6
 *                         (runs `npm run dev -w @seatlock/order-service`; logs go to a temp file)
 *   KAFKA_CONTAINER / ORDERS_DB_CONTAINER   docker container names (defaults seatlock-kafka,
 *                         seatlock-postgres-orders), used only for the optional lag/SQL checks
 *
 * CAVEATS (read these)
 *   - Assumes nothing else is reserving the chaos event while the script runs (stock arithmetic).
 *   - ORDER_SERVICE_PID must be the process that LISTENS on 3002. With `npm run dev` that process is
 *     a child of tsx/concurrently; killing it is enough because the script uses a tree kill, but
 *     `concurrently` may keep running the other services (that is what we want).
 *   - Keep the outage shorter than HOLD_TTL_SECONDS (default 300s). If the outage outlives the hold,
 *     the hold expires and the order correctly ends up EXPIRED instead; see docs/chaos-test.md,
 *     "Variation: outage longer than the hold TTL".
 *   - Payment outcome is random (PAYMENT_FAILURE_RATE, default 20%). Both CONFIRMED and FAILED are
 *     valid; the check is that the services AGREE, not which one happened.
 */

import { spawn, spawnSync } from 'node:child_process';
import { randomUUID } from 'node:crypto';
import { closeSync, openSync } from 'node:fs';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { parseArgs } from 'node:util';

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

const { values: args } = parseArgs({
  options: {
    'dry-run': { type: 'boolean', default: false },
    'skip-payment': { type: 'boolean', default: false },
    'outage-seconds': { type: 'string', default: '20' },
    help: { type: 'boolean', short: 'h', default: false },
  },
  allowPositionals: false,
});

if (args.help) {
  console.log(
    'Usage: tsx scripts/chaos-test.ts [--dry-run] [--skip-payment] [--outage-seconds N]\n' +
      'See the header comment of this file for the full documentation.',
  );
  process.exit(0);
}

const DRY = args['dry-run'] === true;
const OUTAGE_SECONDS = Number(args['outage-seconds']);
if (!Number.isFinite(OUTAGE_SECONDS) || OUTAGE_SECONDS < 0) {
  console.error('--outage-seconds must be a non-negative number');
  process.exit(2);
}

const URLS = {
  inventory: process.env.INVENTORY_URL ?? 'http://localhost:3001',
  order: process.env.ORDER_URL ?? 'http://localhost:3002',
  payment: process.env.PAYMENT_URL ?? 'http://localhost:3003',
  notification: process.env.NOTIFICATION_URL ?? 'http://localhost:3004',
};
const EVENT_ID = process.env.CHAOS_EVENT_ID ?? '00000000-0000-4000-8000-00000000a003';
const ORDER_SERVICE_PID = process.env.ORDER_SERVICE_PID
  ? Number(process.env.ORDER_SERVICE_PID)
  : undefined;
const AUTO_RESTART = process.env.CHAOS_AUTO_RESTART === 'true';
const KAFKA_CONTAINER = process.env.KAFKA_CONTAINER ?? 'seatlock-kafka';
const ORDERS_DB_CONTAINER = process.env.ORDERS_DB_CONTAINER ?? 'seatlock-postgres-orders';
/** Inventory caches GET /events for CACHE_TTL_SECONDS (default 5). Wait slightly longer before re-reading. */
const CACHE_WAIT_MS = 6_500;

const RUN_ID = randomUUID().slice(0, 8);
const IS_WINDOWS = process.platform === 'win32';

// ---------------------------------------------------------------------------
// Output helpers
// ---------------------------------------------------------------------------

const results: Array<{ name: string; ok: boolean; detail: string }> = [];

function banner(text: string): void {
  console.log(`\n${'='.repeat(78)}\n${text}\n${'='.repeat(78)}`);
}

function note(text: string): void {
  console.log(`  ${text}`);
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

/** Record and print one verification. Dry runs record nothing. */
function check(name: string, ok: boolean, detail = ''): boolean {
  results.push({ name, ok, detail });
  console.log(`  [${ok ? 'PASS' : 'FAIL'}] ${name}${detail ? `  (${detail})` : ''}`);
  return ok;
}

// ---------------------------------------------------------------------------
// HTTP
// ---------------------------------------------------------------------------

type Json = any; // eslint-disable-line @typescript-eslint/no-explicit-any

interface HttpResult {
  status: number; // 0 = network error / timeout
  headers: Headers | undefined;
  json: Json;
  error?: string;
}

async function http(
  method: 'GET' | 'POST',
  url: string,
  opts: { headers?: Record<string, string>; body?: unknown } = {},
): Promise<HttpResult> {
  try {
    const res = await fetch(url, {
      method,
      headers: {
        Accept: 'application/json',
        ...(opts.body !== undefined ? { 'Content-Type': 'application/json' } : {}),
        ...opts.headers,
      },
      body: opts.body !== undefined ? JSON.stringify(opts.body) : undefined,
      signal: AbortSignal.timeout(10_000),
    });
    const text = await res.text();
    let json: Json;
    try {
      json = text ? JSON.parse(text) : undefined;
    } catch {
      json = undefined;
    }
    return { status: res.status, headers: res.headers, json };
  } catch (err) {
    return { status: 0, headers: undefined, json: undefined, error: (err as Error).message };
  }
}

/** Render a request the way a human would type it, for the current shell. */
function cmd(
  method: 'GET' | 'POST',
  url: string,
  headers: Record<string, string> = {},
  body?: unknown,
): string {
  if (IS_WINDOWS) {
    const h = Object.entries(headers)
      .map(([k, v]) => `'${k}'='${v}'`)
      .join('; ');
    return (
      `Invoke-RestMethod -Method ${method} -Uri '${url}'` +
      (h ? ` -Headers @{${h}}` : '') +
      (body !== undefined ? ` -ContentType 'application/json' -Body '${JSON.stringify(body)}'` : '')
    );
  }
  return `curl -s ${method === 'POST' ? '-X POST ' : ''}${Object.entries(headers)
    .map(([k, v]) => `-H '${k}: ${v}' `)
    .join(
      '',
    )}${body !== undefined ? `-H 'Content-Type: application/json' -d '${JSON.stringify(body)}' ` : ''}'${url}'`;
}

/** Print the exact command and what it should return. In a dry run this is all that happens. */
function show(commands: string[], expected: string[]): void {
  for (const c of commands) note(`$ ${c}`);
  for (const e of expected) note(`expected: ${e}`);
}

async function waitFor<T>(
  what: string,
  probe: () => Promise<T | undefined>,
  timeoutMs: number,
  intervalMs = 1_000,
): Promise<{ value: T | undefined; elapsedMs: number }> {
  const start = Date.now();
  for (;;) {
    const value = await probe();
    if (value !== undefined) return { value, elapsedMs: Date.now() - start };
    if (Date.now() - start >= timeoutMs) {
      note(`(gave up waiting for: ${what} after ${Math.round(timeoutMs / 1000)}s)`);
      return { value: undefined, elapsedMs: Date.now() - start };
    }
    await sleep(intervalMs);
  }
}

function tryDocker(dockerArgs: string[]): string | undefined {
  const r = spawnSync('docker', dockerArgs, { encoding: 'utf8', timeout: 15_000 });
  return r.status === 0 ? r.stdout : undefined;
}

// ---------------------------------------------------------------------------
// Domain helpers
// ---------------------------------------------------------------------------

interface Ticket {
  label: string;
  userId: string;
  reserveKey: string;
  holdId?: string;
  reservedAt?: number;
  unitPriceCents?: number;
  orderId?: string;
  paymentId?: string;
  finalOrderStatus?: string;
}

function newTicket(label: string): Ticket {
  return { label, userId: randomUUID(), reserveKey: `chaos-${RUN_ID}-${label}-reserve` };
}

/** One id for the whole flow, so `grep chaos-<run>-A` over every service's logs shows the full saga. */
function correlationIdFor(t: Ticket): string {
  return `chaos-${RUN_ID}-${t.label}`;
}

async function reserve(t: Ticket): Promise<HttpResult> {
  const body = { eventId: EVENT_ID, userId: t.userId, quantity: 1 };
  const res = await http('POST', `${URLS.inventory}/reservations`, {
    headers: { 'Idempotency-Key': t.reserveKey, 'X-Correlation-Id': correlationIdFor(t) },
    body,
  });
  if (res.status === 201) {
    t.holdId = res.json.holdId;
    t.unitPriceCents = res.json.unitPriceCents;
    t.reservedAt = Date.now();
  }
  return res;
}

async function ordersForUser(userId: string): Promise<Json[] | undefined> {
  const res = await http('GET', `${URLS.order}/orders?userId=${userId}&limit=100`);
  return res.status === 200 ? (res.json.items as Json[]) : undefined;
}

async function findOrderForHold(t: Ticket): Promise<Json | undefined> {
  const orders = await ordersForUser(t.userId);
  return orders?.find((o) => o.holdId === t.holdId);
}

async function serviceReady(base: string): Promise<boolean> {
  const res = await http('GET', `${base}/health/ready`);
  return res.status === 200;
}

async function serviceAlive(base: string): Promise<boolean> {
  const res = await http('GET', `${base}/health/live`);
  return res.status === 200;
}

async function getAvailable(): Promise<number | undefined> {
  const res = await http('GET', `${URLS.inventory}/events/${EVENT_ID}`);
  return res.status === 200 ? (res.json.available as number) : undefined;
}

// ---------------------------------------------------------------------------
// Killing / restarting order-service
// ---------------------------------------------------------------------------

function killOrderService(pid: number): void {
  if (IS_WINDOWS) {
    // /T kills the whole tree: `npm run dev` -> tsx -> node are separate processes on Windows.
    const r = spawnSync('taskkill', ['/PID', String(pid), '/T', '/F'], { encoding: 'utf8' });
    note(`taskkill exit=${r.status}: ${(r.stdout || r.stderr).trim().split('\n')[0] ?? ''}`);
  } else {
    process.kill(pid, 'SIGKILL'); // SIGKILL on purpose: a crash, not a graceful shutdown
    note(`sent SIGKILL to ${pid}`);
  }
}

function startOrderService(): string {
  const logFile = path.join(tmpdir(), `seatlock-order-service-chaos-${RUN_ID}.log`);
  const fd = openSync(logFile, 'a');
  const child = spawn('npm', ['run', 'dev', '-w', '@seatlock/order-service'], {
    cwd: process.cwd(),
    shell: true, // required for npm on Windows (npm.cmd)
    detached: true,
    stdio: ['ignore', fd, fd],
    windowsHide: true,
  });
  child.unref();
  closeSync(fd);
  return logFile;
}

// ---------------------------------------------------------------------------
// The scenario
// ---------------------------------------------------------------------------

function showPaymentPhase(): void {
  banner(
    'STEP 7b: pay both orders (idempotent retries), wait for terminal state, compare services',
  );
  show(
    [
      cmd(
        'POST',
        `${URLS.order}/orders/<orderId>/pay`,
        { 'Idempotency-Key': `chaos-${RUN_ID}-<A|B>-pay` },
        {},
      ),
      '# repeat with the same key, then once with a different key',
      cmd('GET', `${URLS.order}/orders/<orderId>`),
      cmd('GET', `${URLS.payment}/payments?orderId=<orderId>`),
    ],
    [
      'pay: HTTP 202 {paymentId, paymentStatus:"REQUESTED"}; same key again: 202 + Idempotent-Replayed: true + same paymentId',
      'pay with a different key: HTTP 409 (PAYMENT_ALREADY_INITIATED, or ORDER_NOT_PAYABLE if the payment already settled): no double charge',
      'order -> CONFIRMED (about 80%) or FAILED (about 20%, PAYMENT_FAILURE_RATE) within a few seconds',
      'exactly one payment per order; hold CONFIRMED if order CONFIRMED, RELEASED if FAILED',
      'stock: available = before - 2 + (number of FAILED orders)',
    ],
  );
}

async function main(): Promise<void> {
  banner(
    `SeatLock chaos test  (run ${RUN_ID})${DRY ? '   [DRY RUN: nothing will be called]' : ''}`,
  );
  note(`event under test : ${EVENT_ID}`);
  note(`outage length    : ${OUTAGE_SECONDS}s`);
  note(
    `kill method      : ${ORDER_SERVICE_PID ? `automatic (PID ${ORDER_SERVICE_PID})` : 'manual (you stop it)'}`,
  );
  note(`restart method   : ${AUTO_RESTART ? 'automatic' : 'manual (you start it)'}`);
  if (IS_WINDOWS) note('shell hint        : commands below are PowerShell; on bash use curl.');

  const A = newTicket('A');
  const B = newTicket('B');

  // ------------------------------------------------------------------ Step 1
  banner('STEP 1: health-check all services');
  show(
    Object.values(URLS).map((u) => cmd('GET', `${u}/health/ready`)),
    [
      'HTTP 200 {"status":"ready", ...} from all four services. If one is down, start the stack: npm run dev',
    ],
  );
  if (!DRY) {
    let allReady = true;
    for (const [name, base] of Object.entries(URLS)) {
      const res = await http('GET', `${base}/health/ready`);
      const ok = res.status === 200;
      allReady &&= ok;
      check(
        `${name}-service ready`,
        ok,
        res.status === 0
          ? `unreachable: ${res.error}`
          : `HTTP ${res.status} ${JSON.stringify(res.json?.checks ?? '')}`,
      );
    }
    if (!allReady) {
      console.error('\nAborting: every service must be healthy before injecting a failure.');
      process.exitCode = 1;
      return;
    }
  }

  // ------------------------------------------------------------------ Step 2
  banner('STEP 2: list events (events are seeded; there is no create-event API)');
  show(
    [
      cmd('GET', `${URLS.inventory}/events`),
      '# to (re)create events with full stock:  npm run seed -w @seatlock/inventory-service -- --reset',
    ],
    [`the list contains ${EVENT_ID} with available > 2`],
  );
  let availableBefore: number | undefined;
  if (!DRY) {
    const res = await http('GET', `${URLS.inventory}/events`);
    const ev = (res.json?.events as Json[] | undefined)?.find((e) => e.id === EVENT_ID);
    if (!ev) {
      console.error(`\nAborting: event ${EVENT_ID} not found. Run: npm run seed`);
      process.exitCode = 1;
      return;
    }
    availableBefore = ev.available as number;
    note(
      `"${ev.name}": ${ev.available}/${ev.totalTickets} available at ${ev.unitPriceCents} cents`,
    );
    if (
      !check(
        'event has enough stock for the test',
        availableBefore >= 2,
        `available=${availableBefore}`,
      )
    ) {
      process.exitCode = 1;
      return;
    }
  }

  // ------------------------------------------------------------------ Step 3
  banner('STEP 3: BEFORE the outage: reserve ticket A and wait for its order (control group)');
  show(
    [
      cmd(
        'POST',
        `${URLS.inventory}/reservations`,
        { 'Idempotency-Key': A.reserveKey },
        { eventId: EVENT_ID, userId: A.userId, quantity: 1 },
      ),
      cmd('GET', `${URLS.order}/orders?userId=${A.userId}`),
    ],
    [
      'reservation: HTTP 201 with holdId, expiresAt, status ACTIVE',
      'orders: within ~1-3s one order with holdId = that holdId, status PENDING_PAYMENT (created via Kafka)',
    ],
  );
  if (!DRY) {
    const res = await reserve(A);
    if (!check('reservation A accepted', res.status === 201, `HTTP ${res.status}`)) {
      process.exitCode = 1;
      return;
    }
    const { value, elapsedMs } = await waitFor('order A', () => findOrderForHold(A), 30_000, 500);
    check(
      'order A appeared while order-service was healthy',
      value !== undefined,
      value ? `${elapsedMs}ms` : 'timeout',
    );
    A.orderId = value?.id;
    if (!A.orderId) {
      process.exitCode = 1;
      return;
    }
  }

  // ------------------------------------------------------------------ Step 4
  banner('STEP 4: stop order-service (inject the failure)');
  if (ORDER_SERVICE_PID) {
    show(
      [IS_WINDOWS ? `taskkill /PID ${ORDER_SERVICE_PID} /T /F` : `kill -9 ${ORDER_SERVICE_PID}`],
      ['order-service stops answering on :3002'],
    );
  } else {
    show(
      [
        IS_WINDOWS
          ? 'Get-NetTCPConnection -LocalPort 3002 -State Listen | Select-Object OwningProcess   # find the PID'
          : 'lsof -ti :3002 -sTCP:LISTEN   # find the PID',
        IS_WINDOWS ? 'Stop-Process -Id <PID> -Force' : 'kill -9 <PID>',
        '# or press Ctrl+C in the terminal that runs order-service (with `npm run dev`, Ctrl+C stops ALL services; prefer killing just the PID)',
      ],
      ['order-service stops answering on :3002; the other three keep running'],
    );
  }
  if (!DRY) {
    if (ORDER_SERVICE_PID) {
      killOrderService(ORDER_SERVICE_PID);
    } else {
      note(
        '>>> Stop order-service NOW using one of the commands above. Waiting up to 5 minutes...',
      );
    }
    const down = await waitFor(
      'order-service to go down',
      async () => ((await serviceAlive(URLS.order)) ? undefined : true),
      300_000,
      1_000,
    );
    if (!check('order-service is DOWN', down.value === true)) {
      process.exitCode = 1;
      return;
    }
  }

  // ------------------------------------------------------------------ Step 5
  banner(`STEP 5: DURING the outage: reserve ticket B, retry it, wait ${OUTAGE_SECONDS}s`);
  show(
    [
      cmd(
        'POST',
        `${URLS.inventory}/reservations`,
        { 'Idempotency-Key': B.reserveKey },
        { eventId: EVENT_ID, userId: B.userId, quantity: 1 },
      ),
      '# repeat the SAME command: idempotency check',
      `docker exec ${KAFKA_CONTAINER} /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group order-service`,
    ],
    [
      'first call: HTTP 201 (inventory does not depend on order-service)',
      'second call: HTTP 201, header Idempotent-Replayed: true, SAME holdId, stock not decremented twice',
      'consumer-group output: LAG > 0 on inventory.events (the InventoryReserved event is waiting for order-service)',
    ],
  );
  if (!DRY) {
    const first = await reserve(B);
    check(
      'reservation B accepted while order-service is down',
      first.status === 201,
      `HTTP ${first.status}`,
    );
    if (first.status !== 201) {
      process.exitCode = 1;
      return;
    }
    const replay = await http('POST', `${URLS.inventory}/reservations`, {
      headers: { 'Idempotency-Key': B.reserveKey, 'X-Correlation-Id': correlationIdFor(B) },
      body: { eventId: EVENT_ID, userId: B.userId, quantity: 1 },
    });
    check(
      'retry with same Idempotency-Key replays the same hold',
      replay.status === 201 &&
        replay.headers?.get('idempotent-replayed') === 'true' &&
        replay.json?.holdId === B.holdId,
      `HTTP ${replay.status}, replayed=${replay.headers?.get('idempotent-replayed')}, same holdId=${replay.json?.holdId === B.holdId}`,
    );
    const stillDown = !(await serviceAlive(URLS.order));
    check('order-service is still down (outage is real)', stillDown);

    note(`waiting ${OUTAGE_SECONDS}s so the backlog builds up ...`);
    await sleep(OUTAGE_SECONDS * 1000);

    const lag = tryDocker([
      'exec',
      KAFKA_CONTAINER,
      '/opt/kafka/bin/kafka-consumer-groups.sh',
      '--bootstrap-server',
      'localhost:9092',
      '--describe',
      '--group',
      'order-service',
    ]);
    if (lag) {
      console.log('\n  Consumer lag for group "order-service":\n');
      console.log(
        lag
          .trim()
          .split('\n')
          .map((l) => `    ${l}`)
          .join('\n'),
      );
    } else {
      note(
        '(docker not available from here; check lag in Kafka UI: http://localhost:8080 -> Consumers -> order-service)',
      );
    }
  }

  // ------------------------------------------------------------------ Step 6
  banner('STEP 6: restart order-service');
  const restartCmd = 'npm run dev -w @seatlock/order-service';
  show(
    [
      restartCmd,
      '# (run it in a terminal at the repo root; it applies migrations, then resumes consuming)',
    ],
    [
      'GET :3002/health/ready returns 200 within ~10-30s',
      'its Kafka consumer group resumes from the last committed offset',
    ],
  );
  if (!DRY) {
    if (ORDER_SERVICE_PID && AUTO_RESTART) {
      const logFile = startOrderService();
      note(`started "${restartCmd}" in the background; its log: ${logFile}`);
    } else if (ORDER_SERVICE_PID && !AUTO_RESTART) {
      note(
        `>>> Start it again NOW: ${restartCmd}   (or set CHAOS_AUTO_RESTART=true). Waiting up to 5 minutes...`,
      );
    } else {
      note(`>>> Start it again NOW: ${restartCmd}. Waiting up to 5 minutes...`);
    }
    const up = await waitFor(
      'order-service ready',
      async () => ((await serviceReady(URLS.order)) ? true : undefined),
      300_000,
      1_000,
    );
    if (
      !check(
        'order-service is READY again',
        up.value === true,
        up.value ? `after ${Math.round(up.elapsedMs / 1000)}s` : 'timeout',
      )
    ) {
      process.exitCode = 1;
      return;
    }
  }

  // ------------------------------------------------------------------ Step 7
  banner('STEP 7: verify eventual consistency and no duplicates');
  show(
    [
      cmd('GET', `${URLS.order}/orders?userId=${B.userId}`),
      cmd('GET', `${URLS.inventory}/holds/<holdId of B>`),
      `docker exec ${ORDERS_DB_CONTAINER} psql -U seatlock -d orders -c "SELECT hold_id, COUNT(*) FROM orders GROUP BY hold_id HAVING COUNT(*) > 1;"`,
    ],
    [
      'order for B appears (within a few seconds of restart) with status PENDING_PAYMENT, amountCents = unit price',
      'exactly ONE order per hold, also a few seconds later (no late duplicates from redelivery)',
      'the duplicate-hold SQL returns (0 rows)',
    ],
  );
  const tickets = [A, B];
  if (DRY && !args['skip-payment']) showPaymentPhase();
  if (!DRY) {
    // 7a. Orders exist, exactly once.
    const { value: orderB, elapsedMs } = await waitFor(
      'order B',
      () => findOrderForHold(B),
      90_000,
      1_000,
    );
    check(
      'order B was created after the restart (not lost)',
      orderB !== undefined,
      orderB
        ? `visible ${Math.round(elapsedMs / 1000)}s after order-service was ready; ${Math.round((Date.now() - (B.reservedAt ?? Date.now())) / 1000)}s after the reservation`
        : 'timeout',
    );
    B.orderId = orderB?.id;

    note('letting any duplicate deliveries land (5s) ...');
    await sleep(5_000);

    for (const t of tickets) {
      const orders = (await ordersForUser(t.userId)) ?? [];
      const forHold = orders.filter((o) => o.holdId === t.holdId);
      check(
        `exactly one order for hold ${t.label}`,
        forHold.length === 1,
        `${forHold.length} order(s)`,
      );
      const o = forHold[0];
      if (o) {
        t.orderId = o.id;
        check(
          `order ${t.label} amount = unit price x quantity`,
          o.amountCents === (t.unitPriceCents ?? -1) * 1 && o.status === 'PENDING_PAYMENT',
          `amountCents=${o.amountCents}, status=${o.status}`,
        );
      }
      const hold = await http('GET', `${URLS.inventory}/holds/${t.holdId}`);
      check(
        `hold ${t.label} still ACTIVE (outage shorter than hold TTL)`,
        hold.json?.status === 'ACTIVE',
        `status=${hold.json?.status}, secondsRemaining=${hold.json?.secondsRemaining}`,
      );
    }

    const dup = tryDocker([
      'exec',
      ORDERS_DB_CONTAINER,
      'psql',
      '-U',
      'seatlock',
      '-d',
      'orders',
      '-t',
      '-A',
      '-c',
      'SELECT hold_id, COUNT(*) FROM orders GROUP BY hold_id HAVING COUNT(*) > 1;',
    ]);
    if (dup !== undefined)
      check('SQL: no hold has more than one order', dup.trim() === '', dup.trim() || '0 rows');
    else note('(docker not available; run the duplicate-hold SQL above yourself)');

    if (tickets.some((t) => !t.orderId)) {
      console.error('\nCannot continue to the payment phase: an order is missing.');
      return;
    }

    // 7b. Payment phase.
    if (!args['skip-payment']) {
      showPaymentPhase();

      for (const t of tickets) {
        const key = `chaos-${RUN_ID}-${t.label}-pay`;
        const first = await http('POST', `${URLS.order}/orders/${t.orderId}/pay`, {
          headers: { 'Idempotency-Key': key, 'X-Correlation-Id': correlationIdFor(t) },
          body: {},
        });
        check(`pay ${t.label}: accepted`, first.status === 202, `HTTP ${first.status}`);
        t.paymentId = first.json?.paymentId;

        const again = await http('POST', `${URLS.order}/orders/${t.orderId}/pay`, {
          headers: { 'Idempotency-Key': key },
          body: {},
        });
        check(
          `pay ${t.label}: same key replays, no second payment`,
          again.status === 202 &&
            again.headers?.get('idempotent-replayed') === 'true' &&
            again.json?.paymentId === t.paymentId,
          `HTTP ${again.status}, replayed=${again.headers?.get('idempotent-replayed')}`,
        );

        const other = await http('POST', `${URLS.order}/orders/${t.orderId}/pay`, {
          headers: { 'Idempotency-Key': `${key}-other` },
          body: {},
        });
        check(
          `pay ${t.label}: different key is refused`,
          other.status === 409,
          `HTTP ${other.status} ${other.json?.error?.code ?? ''}`,
        );
      }

      for (const t of tickets) {
        const { value: terminal } = await waitFor(
          `order ${t.label} terminal`,
          async () => {
            const res = await http('GET', `${URLS.order}/orders/${t.orderId}`);
            return res.json && res.json.status !== 'PENDING_PAYMENT'
              ? (res.json as Json)
              : undefined;
          },
          60_000,
          1_000,
        );
        t.finalOrderStatus = terminal?.status;
        check(
          `order ${t.label} reached a terminal state`,
          terminal !== undefined,
          `status=${terminal?.status}`,
        );

        const payments = await http('GET', `${URLS.payment}/payments?orderId=${t.orderId}`);
        const list = (payments.json?.payments as Json[] | undefined) ?? [];
        check(
          `exactly one payment for order ${t.label}`,
          list.length === 1,
          `${list.length} payment(s), status=${list[0]?.status}`,
        );

        const expectedHold =
          t.finalOrderStatus === 'CONFIRMED'
            ? 'CONFIRMED'
            : t.finalOrderStatus === 'FAILED'
              ? 'RELEASED'
              : undefined;
        const { value: holdStatus } = await waitFor(
          `hold ${t.label} to settle`,
          async () => {
            const res = await http('GET', `${URLS.inventory}/holds/${t.holdId}`);
            return res.json?.status !== 'ACTIVE' ? (res.json?.status as string) : undefined;
          },
          30_000,
          1_000,
        );
        check(
          `hold ${t.label} agrees with order ${t.label}`,
          expectedHold !== undefined && holdStatus === expectedHold,
          `order=${t.finalOrderStatus}, hold=${holdStatus}`,
        );
        if (list[0]) {
          const consistent =
            (list[0].status === 'SUCCEEDED') === (t.finalOrderStatus === 'CONFIRMED');
          check(
            `payment ${t.label} agrees with order ${t.label}`,
            consistent,
            `payment=${list[0].status}, order=${t.finalOrderStatus}`,
          );
        }
      }

      // 7c. Stock arithmetic (needs the 5s read cache to roll over).
      note(`waiting ${CACHE_WAIT_MS}ms for the inventory read cache to expire ...`);
      await sleep(CACHE_WAIT_MS);
      const availableAfter = await getAvailable();
      const released = tickets.filter((t) => t.finalOrderStatus === 'FAILED').length;
      const expected = (availableBefore ?? NaN) - tickets.length + released;
      check(
        'stock arithmetic: available = before - reservations + released',
        availableAfter === expected,
        `before=${availableBefore}, reserved=${tickets.length}, released=${released}, expected=${expected}, actual=${availableAfter}`,
      );
    }
  }

  // ------------------------------------------------------------------ Summary
  banner('SUMMARY');
  if (DRY) {
    console.log(
      '  Dry run only. Re-run without --dry-run (and with the stack up) to execute the scenario.',
    );
    return;
  }
  const failed = results.filter((r) => !r.ok);
  console.log(`  ${results.length - failed.length}/${results.length} checks passed`);
  for (const f of failed) console.log(`  FAILED: ${f.name} (${f.detail})`);
  console.log(
    failed.length === 0
      ? '\n  RESULT: PASS. An order-service crash delayed order creation but lost nothing and duplicated nothing.'
      : '\n  RESULT: FAIL. See docs/chaos-test.md "If a check fails" for where to look.',
  );
  console.log(
    `\n  Test data: holdIds A=${A.holdId} B=${B.holdId}; userIds A=${A.userId} B=${B.userId}`,
  );
  console.log(
    `  Tip: follow one flow end to end by searching every service's logs for "chaos-${RUN_ID}-A" or "chaos-${RUN_ID}-B" (the X-Correlation-Id sent on reserve and pay).`,
  );
  if (failed.length > 0) process.exitCode = 1;
}

main().catch((err: unknown) => {
  console.error(`chaos-test crashed: ${(err as Error).stack ?? err}`);
  process.exitCode = 1;
});
