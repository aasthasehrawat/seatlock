/**
 * k6 load test: flash-sale reservation stampede.
 *
 * SCENARIO
 *   1000 virtual users (VUs) all try to reserve tickets for ONE event at the same moment. The seed
 *   event has exactly 100 tickets. If the inventory logic is correct:
 *     - exactly 100 reservations succeed        (HTTP 201)
 *     - exactly 900 attempts are told sold out  (HTTP 409, error.code = SOLD_OUT)
 *     - nothing else happens: no 5xx, no 429, no oversell, stock ends at exactly 0.
 *
 * WHAT IS (AND IS NOT) ASSERTED
 *   Thresholds are CORRECTNESS gates, not performance claims. Latency thresholds are off unless you
 *   opt in (see P95_MS). Whatever numbers a run produces are for you to record in docs/load-test.md.
 *
 * PREREQUISITES (all of them matter, see docs/load-test.md for the why)
 *   1. Infra + services running:        docker compose up -d && npm run dev:services
 *   2. Schema + seed with FULL stock:   npm run migrate && npm run seed -w @seatlock/inventory-service -- --reset
 *      (the seed creates event 00000000-0000-4000-8000-00000000a002 with exactly 100 tickets)
 *   3. Rate limiter raised: inventory-service limits each client IP to RATE_LIMIT_MAX requests per
 *      RATE_LIMIT_WINDOW_SECONDS (default 20 per 60s). k6 sends everything from ONE IP, so with the
 *      defaults ~all requests would get HTTP 429 and this test would (correctly) fail. Start
 *      inventory-service with e.g. RATE_LIMIT_MAX=1000000 for the run. Only inventory-service reads
 *      it, and it does not load .env itself, so set it in the shell that starts the service.
 *   4. Wait > 5s after reseeding (the GET /events/:id read cache has a 5s TTL) and clear old
 *      rate-limit counters if you ran before:  docker exec seatlock-redis redis-cli FLUSHDB
 *
 * RUN
 *   npm run load-test                                   (same as: k6 run tests/load/reservation-load.js)
 *   k6 run -e BASE_URL=http://localhost:3001 -e VUS=1000 tests/load/reservation-load.js
 *
 * ENVIRONMENT (k6: pass with -e NAME=value)
 *   BASE_URL            inventory-service URL                    default http://localhost:3001
 *   EVENT_ID            event to attack                          default 00000000-0000-4000-8000-00000000a002
 *   TICKETS             tickets the event starts with            default 100 (setup() aborts if the
 *                       event does not currently have exactly this many available)
 *   VUS                 concurrent virtual users                 default 1000
 *   ITERATIONS          reservation attempts per VU              default 1 (so attempts = VUS * ITERATIONS)
 *   QUANTITY            tickets per reservation                  default 1
 *   REPLAY_CHECK        "true"/"false": re-send each successful reservation once with the SAME
 *                       Idempotency-Key and assert it replays instead of buying again  default true
 *   CACHE_TTL_SECONDS   inventory read-cache TTL, used by teardown to wait out the cache  default 5
 *   P95_MS              OPTIONAL: if set, adds the threshold http_req_duration p(95) < P95_MS
 *   MAX_DURATION        safety cap for the whole scenario        default 2m
 *
 * HOW "EXACT" IS ENFORCED
 *   expectedSuccess = min(floor(TICKETS / QUANTITY), VUS * ITERATIONS)
 *   expectedSoldOut = attempts - expectedSuccess
 *   Thresholds require reservations_success == expectedSuccess and reservations_sold_out ==
 *   expectedSoldOut, plus zero unexpected statuses, zero 429s, zero oversell, and (in teardown) the
 *   event's `available` equal to TICKETS - expectedSuccess * QUANTITY.
 *
 * OUTPUT
 *   A plain-text summary on stdout ready to paste into docs/load-test.md, and the full raw k6 summary
 *   JSON in docs/load-test-raw.json (git-ignored).
 */

import http from 'k6/http';
import { check, sleep } from 'k6';
import { Counter, Gauge, Trend } from 'k6/metrics';

// ---------------------------------------------------------------------------
// Parameters
// ---------------------------------------------------------------------------

const BASE_URL = __ENV.BASE_URL || 'http://localhost:3001';
const EVENT_ID = __ENV.EVENT_ID || '00000000-0000-4000-8000-00000000a002';
const TICKETS = intEnv('TICKETS', 100);
const VUS = intEnv('VUS', 1000);
const ITERATIONS = intEnv('ITERATIONS', 1);
const QUANTITY = intEnv('QUANTITY', 1);
const REPLAY_CHECK = (__ENV.REPLAY_CHECK || 'true') === 'true';
const CACHE_TTL_SECONDS = intEnv('CACHE_TTL_SECONDS', 5);
const MAX_DURATION = __ENV.MAX_DURATION || '2m';

const ATTEMPTS = VUS * ITERATIONS;
const EXPECTED_SUCCESS = Math.min(Math.floor(TICKETS / QUANTITY), ATTEMPTS);
const EXPECTED_SOLD_OUT = ATTEMPTS - EXPECTED_SUCCESS;
const EXPECTED_FINAL_AVAILABLE = TICKETS - EXPECTED_SUCCESS * QUANTITY;

function intEnv(name, fallback) {
  const raw = __ENV[name];
  if (raw === undefined || raw === '') return fallback;
  const n = Number(raw);
  if (!Number.isInteger(n) || n < 0) {
    throw new Error(`${name} must be a non-negative integer (got "${raw}")`);
  }
  return n;
}

// ---------------------------------------------------------------------------
// Metrics: one counter per possible outcome so nothing can hide in "other".
// ---------------------------------------------------------------------------

const attempted = new Counter('reservations_attempted');
const success = new Counter('reservations_success'); //          201
const soldOut = new Counter('reservations_sold_out'); //         409 SOLD_OUT
const rateLimited = new Counter('reservations_rate_limited'); // 429 (means the test setup is wrong)
const unexpected = new Counter('reservations_unexpected'); //    anything else (5xx, other 4xx, bad body, timeout)
const replayMismatch = new Counter('reservation_replay_mismatch'); // idempotent replay did NOT behave as a replay
const replaysChecked = new Counter('reservation_replays_checked');
const verificationFailed = new Counter('verification_failed'); // teardown could not read the final stock
const finalAvailable = new Gauge('final_available'); //           stock left after the run (from teardown)

const successDuration = new Trend('reservation_success_duration', true);
const soldOutDuration = new Trend('reservation_sold_out_duration', true);

// 409 SOLD_OUT is an EXPECTED business outcome here, so it must not count as an HTTP failure.
// What remains in http_req_failed is then genuinely bad: 429, 5xx, other 4xx, connection errors.
http.setResponseCallback(http.expectedStatuses(201, 409));

// ---------------------------------------------------------------------------
// Options and thresholds
// ---------------------------------------------------------------------------

const thresholds = {
  // Hard stop the instant we oversell: this must never trip.
  reservations_success: [
    { threshold: `count<=${Math.floor(TICKETS / QUANTITY)}`, abortOnFail: true },
    `count==${EXPECTED_SUCCESS}`,
  ],
  reservations_sold_out: [`count==${EXPECTED_SOLD_OUT}`],
  reservations_attempted: [`count==${ATTEMPTS}`],
  reservations_rate_limited: ['count==0'],
  reservations_unexpected: ['count==0'],
  reservation_replay_mismatch: ['count==0'],
  verification_failed: ['count==0'],
  final_available: [`value==${EXPECTED_FINAL_AVAILABLE}`],
  http_req_failed: ['rate==0'],
};
if (__ENV.P95_MS) {
  thresholds.http_req_duration = [`p(95)<${Number(__ENV.P95_MS)}`];
}

export const options = {
  scenarios: {
    flash_sale: {
      // Every VU performs its attempt(s) as soon as the test starts: a stampede, not a ramp.
      executor: 'per-vu-iterations',
      vus: VUS,
      iterations: ITERATIONS,
      maxDuration: MAX_DURATION,
    },
  },
  thresholds,
  // So p(99) is available in the summary.
  summaryTrendStats: ['avg', 'min', 'med', 'max', 'p(90)', 'p(95)', 'p(99)'],
  // Teardown waits for the read cache to expire; give it room.
  teardownTimeout: '60s',
};

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/** RFC 4122 v4-shaped id without remote imports (the API requires UUID userIds). */
function uuid() {
  return 'xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx'.replace(/[xy]/g, (c) => {
    const r = (Math.random() * 16) | 0;
    return (c === 'x' ? r : (r & 0x3) | 0x8).toString(16);
  });
}

function tryJson(res) {
  try {
    return res.json();
  } catch (_) {
    return undefined;
  }
}

function getEvent() {
  return http.get(`${BASE_URL}/events/${EVENT_ID}`, { tags: { name: 'GET /events/:id' } });
}

// ---------------------------------------------------------------------------
// Lifecycle
// ---------------------------------------------------------------------------

export function setup() {
  const res = getEvent();
  if (res.status !== 200) {
    throw new Error(
      `GET ${BASE_URL}/events/${EVENT_ID} returned ${res.status}. Are the services up and the DB seeded? ` +
        '(npm run migrate && npm run seed -w @seatlock/inventory-service -- --reset)',
    );
  }
  const ev = res.json();
  if (ev.available !== TICKETS) {
    throw new Error(
      `Event "${ev.name}" has ${ev.available}/${ev.totalTickets} tickets available but this test expects exactly ${TICKETS}. ` +
        'Reseed:  npm run seed -w @seatlock/inventory-service -- --reset   then wait ' +
        `${CACHE_TTL_SECONDS + 1}s for the read cache to expire (or set TICKETS=<n> to match).`,
    );
  }
  const runId = `${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 8)}`;
  console.log(
    `Starting: ${VUS} VUs x ${ITERATIONS} attempt(s) = ${ATTEMPTS} reservations of ${QUANTITY} ticket(s) ` +
      `against "${ev.name}" (${ev.available} available). Expect ${EXPECTED_SUCCESS} success / ${EXPECTED_SOLD_OUT} sold out. runId=${runId}`,
  );
  return { runId };
}

export default function (data) {
  const idempotencyKey = `${data.runId}-vu${__VU}-it${__ITER}`;
  const payload = JSON.stringify({ eventId: EVENT_ID, userId: uuid(), quantity: QUANTITY });
  const params = {
    headers: { 'Content-Type': 'application/json', 'Idempotency-Key': idempotencyKey },
    tags: { name: 'POST /reservations' },
  };

  attempted.add(1);
  const res = http.post(`${BASE_URL}/reservations`, payload, params);
  const body = tryJson(res);

  if (res.status === 201 && body && body.status === 'ACTIVE' && body.holdId) {
    success.add(1);
    successDuration.add(res.timings.duration);

    if (REPLAY_CHECK) {
      // Same key + same body must return the SAME hold without taking more stock.
      const again = http.post(`${BASE_URL}/reservations`, payload, {
        ...params,
        tags: { name: 'POST /reservations (idempotent replay)' },
      });
      const againBody = tryJson(again);
      replaysChecked.add(1);
      const ok =
        again.status === 201 &&
        again.headers['Idempotent-Replayed'] === 'true' &&
        againBody !== undefined &&
        againBody.holdId === body.holdId;
      if (!ok) replayMismatch.add(1);
    }
  } else if (res.status === 409 && body && body.error && body.error.code === 'SOLD_OUT') {
    soldOut.add(1);
    soldOutDuration.add(res.timings.duration);
  } else if (res.status === 429) {
    rateLimited.add(1);
    if (__VU === 1) {
      console.error(
        'HTTP 429: the per-IP rate limiter is rejecting the load. Restart inventory-service with RATE_LIMIT_MAX=1000000 ' +
          '(and run: docker exec seatlock-redis redis-cli FLUSHDB). See docs/load-test.md.',
      );
    }
  } else {
    unexpected.add(1);
    if (__VU <= 3) {
      console.error(
        `unexpected response: status=${res.status} body=${String(res.body).slice(0, 200)}`,
      );
    }
  }

  check(res, { 'status is 201 or 409': (r) => r.status === 201 || r.status === 409 });
  if (ITERATIONS > 1) sleep(0.1);
}

export function teardown() {
  // GET /events/:id is cached for CACHE_TTL_SECONDS. Wait it out so we read the authoritative stock.
  sleep(CACHE_TTL_SECONDS + 1.5);
  const res = getEvent();
  if (res.status !== 200) {
    verificationFailed.add(1);
    console.error(`teardown: could not read final stock (HTTP ${res.status})`);
    return;
  }
  const ev = res.json();
  finalAvailable.add(ev.available);
  console.log(
    `teardown: final stock = ${ev.available}/${ev.totalTickets} available (expected ${EXPECTED_FINAL_AVAILABLE})`,
  );
}

// ---------------------------------------------------------------------------
// Summary (paste-ready; no invented numbers: everything printed comes from this run)
// ---------------------------------------------------------------------------

function metricValue(data, name, stat) {
  const m = data.metrics[name];
  return m && m.values && m.values[stat] !== undefined ? m.values[stat] : undefined;
}

function count(data, name) {
  const v = metricValue(data, name, 'count');
  return v === undefined ? 0 : v;
}

function fmtMs(v) {
  return v === undefined ? 'n/a' : `${v.toFixed(1)} ms`;
}

function trendLine(data, name) {
  const g = (s) => metricValue(data, name, s);
  if (g('med') === undefined) return 'no samples';
  return `avg ${fmtMs(g('avg'))} | med ${fmtMs(g('med'))} | p90 ${fmtMs(g('p(90)'))} | p95 ${fmtMs(g('p(95)'))} | p99 ${fmtMs(g('p(99)'))} | max ${fmtMs(g('max'))}`;
}

function thresholdReport(data) {
  const lines = [];
  for (const [metric, m] of Object.entries(data.metrics)) {
    if (!m.thresholds) continue;
    for (const [expr, res] of Object.entries(m.thresholds)) {
      lines.push(`  ${res.ok ? 'PASS' : 'FAIL'}  ${metric}: ${expr}`);
    }
  }
  return lines.join('\n');
}

export function handleSummary(data) {
  const ok = count(data, 'reservations_success');
  const so = count(data, 'reservations_sold_out');
  const rl = count(data, 'reservations_rate_limited');
  const un = count(data, 'reservations_unexpected');
  const att = count(data, 'reservations_attempted');
  const allPassed = Object.values(data.metrics).every(
    (m) => !m.thresholds || Object.values(m.thresholds).every((t) => t.ok),
  );
  const durationS =
    data.state && data.state.testRunDurationMs ? data.state.testRunDurationMs / 1000 : undefined;
  const reqRate = metricValue(data, 'http_reqs', 'rate');

  const text = [
    '',
    '=== SeatLock reservation load test: results (paste into docs/load-test.md) ===',
    `Target            : ${BASE_URL}  event ${EVENT_ID}`,
    `Load              : ${VUS} VUs x ${ITERATIONS} attempt(s) = ${ATTEMPTS} reservations, quantity ${QUANTITY}, tickets available at start ${TICKETS}`,
    `Replay check      : ${REPLAY_CHECK ? `on (${count(data, 'reservation_replays_checked')} replays checked)` : 'off'}`,
    `Test duration     : ${durationS === undefined ? 'n/a' : `${durationS.toFixed(1)} s (includes ~${CACHE_TTL_SECONDS + 1.5}s teardown wait)`}`,
    '',
    'Outcomes (first attempt per VU iteration)',
    `  attempted       : ${att}`,
    `  success (201)   : ${ok}   expected ${EXPECTED_SUCCESS}`,
    `  sold out (409)  : ${so}   expected ${EXPECTED_SOLD_OUT}`,
    `  rate limited    : ${rl}   expected 0${rl > 0 ? '   <-- raise RATE_LIMIT_MAX, see docs/load-test.md' : ''}`,
    `  unexpected      : ${un}   expected 0`,
    `  oversold        : ${ok > Math.floor(TICKETS / QUANTITY) ? `YES (${ok - Math.floor(TICKETS / QUANTITY)} too many)` : 'no'}`,
    `  replay mismatch : ${count(data, 'reservation_replay_mismatch')}   expected 0`,
    `  final stock     : ${metricValue(data, 'final_available', 'value') === undefined ? 'not read' : metricValue(data, 'final_available', 'value')}   expected ${EXPECTED_FINAL_AVAILABLE}`,
    '',
    'Latency (POST /reservations)',
    `  success         : ${trendLine(data, 'reservation_success_duration')}`,
    `  sold out        : ${trendLine(data, 'reservation_sold_out_duration')}`,
    `  all requests    : ${trendLine(data, 'http_req_duration')}`,
    '',
    `Throughput        : ${reqRate === undefined ? 'n/a' : `${reqRate.toFixed(1)} req/s`} (http_reqs rate over the whole run, including setup/teardown)`,
    `HTTP failed rate  : ${metricValue(data, 'http_req_failed', 'rate') === undefined ? 'n/a' : `${(metricValue(data, 'http_req_failed', 'rate') * 100).toFixed(2)}%`} (409 SOLD_OUT is treated as expected)`,
    '',
    'Thresholds',
    thresholdReport(data),
    '',
    `VERDICT: ${allPassed ? 'PASS: exactly the available tickets were sold, everyone else got SOLD_OUT, nothing oversold.' : 'FAIL: see failed thresholds above.'}`,
    '',
    'Also verify in Postgres (authoritative):',
    '  docker exec seatlock-postgres-inventory psql -U seatlock -d inventory -c "SELECT available, total_tickets FROM events WHERE id = \'' +
      EVENT_ID +
      '\';"',
    '  docker exec seatlock-postgres-inventory psql -U seatlock -d inventory -c "SELECT status, COUNT(*), SUM(quantity) FROM holds WHERE event_id = \'' +
      EVENT_ID +
      '\' GROUP BY status;"',
    '',
  ].join('\n');

  return {
    stdout: text,
    'docs/load-test-raw.json': JSON.stringify(data, null, 2),
  };
}
