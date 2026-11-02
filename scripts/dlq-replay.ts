/**
 * dlq-replay.ts: inspect and replay messages from the SeatLock dead-letter queue (topic `dlq`).
 *
 * WHAT LANDS IN THE DLQ
 *   - inventory-service / order-service: malformed messages, business-rule rejections that retrying
 *     cannot fix (e.g. confirming a hold that already expired), and messages that exhausted their
 *     retries on `retry.5s`.
 *   - payment-service / notification-service: malformed messages and notifications whose hold owner
 *     could not be resolved.
 *   The original key and value are always preserved byte for byte, so a replay re-runs exactly the
 *   same processing.
 *
 * USAGE (run from the repo root)
 *   npm run dlq:inspect                                   # newest 50 messages, human readable
 *   npm run dlq:inspect -- --limit 0                      # everything
 *   npm run dlq:inspect -- --type PaymentSucceeded        # filter by event type
 *   npm run dlq:inspect -- --grep INVALID_STATE           # filter by failure reason / error text
 *   npm run dlq:inspect -- --json                         # one JSON object per line (jq-friendly)
 *
 *   npm run dlq:replay -- --dry-run --all                 # show what WOULD be replayed and where
 *   npm run dlq:replay -- --offset 3 --yes                # replay exactly one message (partition 0)
 *   npm run dlq:replay -- --partition 0 --offset 3 --yes  # same, explicit partition
 *   npm run dlq:replay -- --all --yes                     # replay everything that is routable
 *   npm run dlq:replay -- --type OrderFailed --all --yes  # replay only one event type
 *   npm run dlq:replay -- --offset 3 --topic orders.events --yes   # override the target topic
 *
 *   Without npm:  tsx scripts/dlq-replay.ts inspect [options]
 *                 tsx scripts/dlq-replay.ts replay  [options]
 *
 * OPTIONS
 *   --limit <n>        inspect: show the newest n messages (default 50, 0 = all)
 *   --type <EventType> only messages whose event type matches (e.g. OrderConfirmed)
 *   --grep <text>      only messages whose failure headers contain <text> (case-insensitive)
 *   --partition <p>    only messages from DLQ partition p
 *   --offset <o>       only the message at DLQ offset o (combine with --partition if the DLQ has >1)
 *   --all              replay: select every message that passes the other filters
 *   --topic <name>     replay: force the target topic (must be inventory/orders/payments/notifications .events)
 *   --dry-run          replay: print the plan, publish nothing
 *   --yes              replay: required to actually publish (a safety catch against fat-fingering)
 *   --json             inspect: machine-readable output
 *
 * ENVIRONMENT
 *   KAFKA_BROKERS  broker list (default localhost:9092)
 *   DLQ_TOPIC      dead-letter topic name (default "dlq")
 *
 * HOW THE TARGET TOPIC IS CHOSEN (first match wins)
 *   1. --topic override
 *   2. header `x-original-topic`        (written by order-service)
 *   3. header `dlq-original-topic`      (written by inventory-service)
 *   4. header `sourceTopic`             (written by payment-service and notification-service)
 *   5. the event type, from header `event-type` / `eventType` / `type`, or from the envelope JSON:
 *        Inventory* / HoldExpired / WaitlistOffered  -> inventory.events
 *        Order*                                      -> orders.events
 *        Payment*                                    -> payments.events
 *        NotificationRequested                       -> notifications.events
 *   Only the four *.events topics are valid targets. `retry.*` and `dlq` are rejected so a replay
 *   can never create a loop. A message with no resolvable target is skipped and reported.
 *
 * IS REPLAYING SAFE?
 *   Yes, by design, but understand why:
 *   - Every consumer records the envelope eventId in `processed_events` in the SAME transaction as
 *     its business change. A message that was dead-lettered never committed that row, so the
 *     replay is processed normally. If you replay a message that was in fact already processed,
 *     the consumer sees the duplicate eventId and skips it.
 *   - The replayed message is published to the original topic, so EVERY consumer group on that
 *     topic sees it again. Groups that already handled it dedupe it; a group that had dead-lettered
 *     it gets its second chance.
 *   - Replaying fixes TRANSIENT causes (a dependency was down, a bug was deployed since). A message
 *     that fails for a permanent reason (e.g. the hold really has expired) will simply be
 *     dead-lettered again. Fix the cause first, then replay.
 *   - The DLQ is an append-only Kafka log: replaying does NOT delete the DLQ entry. Replayed
 *     copies carry `x-replayed-at` and `x-replayed-from-dlq-*` headers for traceability.
 *
 * Inspecting never disturbs anything: it reads with a throw-away consumer group and commits no
 * offsets.
 */

import { Kafka, logLevel, type IHeaders, type Producer } from 'kafkajs';
import { randomUUID } from 'node:crypto';
import { parseArgs } from 'node:util';

// ---------------------------------------------------------------------------
// Constants (duplicated from @seatlock/shared on purpose: this script must run before `npm run
// build:shared` and must keep working even if the shared package is mid-refactor).
// ---------------------------------------------------------------------------

const DLQ_TOPIC = process.env.DLQ_TOPIC ?? 'dlq';

const REPLAYABLE_TOPICS = [
  'inventory.events',
  'orders.events',
  'payments.events',
  'notifications.events',
] as const;
type ReplayableTopic = (typeof REPLAYABLE_TOPICS)[number];

/** Header names written by each service when it dead-letters a message. */
const ORIGINAL_TOPIC_HEADERS = ['x-original-topic', 'dlq-original-topic', 'sourceTopic'] as const;
const EVENT_TYPE_HEADERS = ['event-type', 'eventType', 'type'] as const;

/**
 * Headers that describe the FAILURE rather than the event. They are dropped on replay so the
 * republished message looks like a fresh one, not like something that already failed.
 */
const FAILURE_HEADER_PREFIXES = ['x-retry-', 'x-failed-', 'x-original-', 'retry-', 'dlq-'] as const;
const FAILURE_HEADERS_EXACT = new Set(['x-error', 'sourceTopic', 'reason', 'service', 'failedAt']);

// ---------------------------------------------------------------------------
// Types and small helpers
// ---------------------------------------------------------------------------

interface DlqMessage {
  partition: number;
  offset: string;
  timestamp: string;
  key: Buffer | null;
  value: Buffer | null;
  headers: Record<string, string>;
  /** Event type from headers, falling back to the envelope body. */
  eventType: string | undefined;
  /** Envelope fields, when the body parses as JSON. */
  envelope:
    | { eventId?: string; correlationId?: string; type?: string; producer?: string }
    | undefined;
}

function headerValue(h: IHeaders | undefined, name: string): string | undefined {
  const raw = h?.[name];
  const first = Array.isArray(raw) ? raw[0] : raw;
  return first === undefined ? undefined : first.toString();
}

function flattenHeaders(h: IHeaders | undefined): Record<string, string> {
  const out: Record<string, string> = {};
  for (const name of Object.keys(h ?? {})) {
    const v = headerValue(h, name);
    if (v !== undefined) out[name] = v;
  }
  return out;
}

function tryParseEnvelope(value: Buffer | null): DlqMessage['envelope'] {
  if (!value) return undefined;
  try {
    const json = JSON.parse(value.toString('utf8')) as Record<string, unknown>;
    if (json === null || typeof json !== 'object') return undefined;
    const pick = (k: string): string | undefined =>
      typeof json[k] === 'string' ? (json[k] as string) : undefined;
    return {
      eventId: pick('eventId'),
      correlationId: pick('correlationId'),
      type: pick('type'),
      producer: pick('producer'),
    };
  } catch {
    return undefined;
  }
}

function firstHeader(
  headers: Record<string, string>,
  names: readonly string[],
): string | undefined {
  for (const n of names) if (headers[n]) return headers[n];
  return undefined;
}

/** A short, service-agnostic description of why the message is here. */
function failureSummary(m: DlqMessage): {
  service: string;
  reason: string;
  error: string;
  attempts: string;
} {
  const h = m.headers;
  return {
    service: h['x-failed-service'] ?? h['dlq-consumer'] ?? h['service'] ?? '?',
    reason: h['dlq-reason'] ?? h['reason'] ?? (h['x-error'] ? h['x-error'].split(':')[0]! : '?'),
    error: h['x-error'] ?? h['dlq-error'] ?? h['reason'] ?? '',
    attempts: h['dlq-attempts'] ?? h['x-retry-attempt'] ?? '',
  };
}

/** Everything searchable by --grep. */
function searchableText(m: DlqMessage): string {
  const f = failureSummary(m);
  return [f.service, f.reason, f.error, m.eventType ?? '', JSON.stringify(m.headers)]
    .join(' ')
    .toLowerCase();
}

// ---------------------------------------------------------------------------
// Target topic resolution
// ---------------------------------------------------------------------------

function topicForEventType(type: string | undefined): ReplayableTopic | undefined {
  if (!type) return undefined;
  if (type.startsWith('Inventory') || type === 'HoldExpired' || type === 'WaitlistOffered')
    return 'inventory.events';
  if (type.startsWith('Order')) return 'orders.events';
  if (type.startsWith('Payment')) return 'payments.events';
  if (type === 'NotificationRequested') return 'notifications.events';
  return undefined;
}

function isReplayableTopic(t: string | undefined): t is ReplayableTopic {
  return t !== undefined && (REPLAYABLE_TOPICS as readonly string[]).includes(t);
}

interface Resolution {
  topic?: ReplayableTopic;
  via: string;
  problem?: string;
}

function resolveTarget(m: DlqMessage, override: string | undefined): Resolution {
  if (override !== undefined) {
    return isReplayableTopic(override)
      ? { topic: override, via: '--topic override' }
      : {
          via: '--topic override',
          problem: `"${override}" is not one of ${REPLAYABLE_TOPICS.join(', ')}`,
        };
  }

  for (const name of ORIGINAL_TOPIC_HEADERS) {
    const candidate = m.headers[name];
    if (candidate === undefined) continue;
    if (isReplayableTopic(candidate)) return { topic: candidate, via: `header ${name}` };
    // e.g. "retry.5s" or "unknown": do not trust it, fall through to the event type.
  }

  const type = m.eventType;
  const inferred = topicForEventType(type);
  if (inferred) return { topic: inferred, via: `event type ${type}` };

  return {
    via: 'none',
    problem: 'no usable original-topic header and unknown event type; use --topic <name>',
  };
}

// ---------------------------------------------------------------------------
// Reading the DLQ
// ---------------------------------------------------------------------------

function makeKafka(): Kafka {
  const brokers = (process.env.KAFKA_BROKERS ?? 'localhost:9092')
    .split(',')
    .map((b) => b.trim())
    .filter(Boolean);
  return new Kafka({
    clientId: `seatlock-dlq-tool-${randomUUID().slice(0, 8)}`,
    brokers,
    logLevel: logLevel.NOTHING,
    retry: { retries: 3, initialRetryTime: 300 },
  });
}

/**
 * Read every message currently in the DLQ (up to the high watermark observed at start) and
 * return them. Uses a unique consumer group with auto-commit OFF, so it never moves any real
 * group's offsets and leaves no state behind.
 */
async function readAllDlq(kafka: Kafka): Promise<DlqMessage[]> {
  const admin = kafka.admin();
  await admin.connect();

  let offsets: Array<{ partition: number; low: string; high: string }>;
  try {
    offsets = await admin.fetchTopicOffsets(DLQ_TOPIC);
  } catch (err) {
    await admin.disconnect();
    throw new Error(
      `Cannot read topic "${DLQ_TOPIC}": ${(err as Error).message}\n` +
        'Does it exist? Create all topics with: npm run setup:topics',
    );
  }
  await admin.disconnect();

  // Partitions that actually contain something, and the last offset we must see in each.
  const lastWanted = new Map<number, bigint>();
  for (const o of offsets) {
    if (BigInt(o.high) > BigInt(o.low)) lastWanted.set(o.partition, BigInt(o.high) - 1n);
  }
  if (lastWanted.size === 0) return [];

  const consumer = kafka.consumer({ groupId: `dlq-tool-${randomUUID()}` });
  const messages: DlqMessage[] = [];
  const pending = new Set(lastWanted.keys());

  await consumer.connect();
  await consumer.subscribe({ topic: DLQ_TOPIC, fromBeginning: true });

  await new Promise<void>((resolve, reject) => {
    const timer = setTimeout(
      () => reject(new Error(`Timed out reading "${DLQ_TOPIC}" (30s). Is the broker healthy?`)),
      30_000,
    );
    consumer
      .run({
        autoCommit: false,
        eachMessage: async ({ partition, message }) => {
          const headers = flattenHeaders(message.headers);
          const envelope = tryParseEnvelope(message.value);
          messages.push({
            partition,
            offset: message.offset,
            timestamp: new Date(Number(message.timestamp)).toISOString(),
            key: message.key,
            value: message.value,
            headers,
            envelope,
            eventType: firstHeader(headers, EVENT_TYPE_HEADERS) ?? envelope?.type,
          });
          const wanted = lastWanted.get(partition);
          if (wanted !== undefined && BigInt(message.offset) >= wanted) {
            pending.delete(partition);
            if (pending.size === 0) {
              clearTimeout(timer);
              resolve();
            }
          }
        },
      })
      .catch((err: unknown) => {
        clearTimeout(timer);
        reject(err);
      });
  }).finally(async () => {
    await consumer.disconnect().catch(() => undefined);
  });

  messages.sort((a, b) => a.partition - b.partition || Number(BigInt(a.offset) - BigInt(b.offset)));
  return messages;
}

// ---------------------------------------------------------------------------
// Commands
// ---------------------------------------------------------------------------

interface Filters {
  type: string | undefined;
  grep: string | undefined;
  partition: number | undefined;
  offset: string | undefined;
}

function applyFilters(all: DlqMessage[], f: Filters): DlqMessage[] {
  return all.filter((m) => {
    if (f.type !== undefined && m.eventType !== f.type) return false;
    if (f.partition !== undefined && m.partition !== f.partition) return false;
    if (f.offset !== undefined && m.offset !== f.offset) return false;
    if (f.grep !== undefined && !searchableText(m).includes(f.grep.toLowerCase())) return false;
    return true;
  });
}

function preview(value: Buffer | null, max = 160): string {
  if (!value) return '(empty)';
  const text = value.toString('utf8').replace(/\s+/g, ' ');
  return text.length > max ? `${text.slice(0, max)}...` : text;
}

function printInspect(messages: DlqMessage[], total: number, json: boolean): void {
  if (json) {
    for (const m of messages) {
      const f = failureSummary(m);
      console.log(
        JSON.stringify({
          partition: m.partition,
          offset: m.offset,
          timestamp: m.timestamp,
          key: m.key?.toString('utf8') ?? null,
          eventType: m.eventType ?? null,
          eventId: m.envelope?.eventId ?? null,
          correlationId: m.envelope?.correlationId ?? null,
          failedService: f.service,
          reason: f.reason,
          error: f.error,
          attempts: f.attempts,
          resolvedTarget: resolveTarget(m, undefined).topic ?? null,
          headers: m.headers,
          value: m.value?.toString('utf8') ?? null,
        }),
      );
    }
    return;
  }

  if (messages.length === 0) {
    console.log(
      total === 0
        ? `The DLQ (${DLQ_TOPIC}) is empty.`
        : `No messages match the filters (${total} in the DLQ).`,
    );
    return;
  }

  console.log(`DLQ "${DLQ_TOPIC}": showing ${messages.length} of ${total} message(s)\n`);
  for (const m of messages) {
    const f = failureSummary(m);
    const target = resolveTarget(m, undefined);
    console.log(`partition ${m.partition}  offset ${m.offset}  ${m.timestamp}`);
    console.log(`  event type : ${m.eventType ?? '(unknown)'}`);
    console.log(`  event id   : ${m.envelope?.eventId ?? '(unparseable body)'}`);
    console.log(`  correlation: ${m.envelope?.correlationId ?? '-'}`);
    console.log(`  key        : ${m.key?.toString('utf8') ?? '-'}`);
    console.log(
      `  failed in  : ${f.service}   reason: ${f.reason}${f.attempts ? `   attempts: ${f.attempts}` : ''}`,
    );
    if (f.error) console.log(`  error      : ${f.error.slice(0, 300)}`);
    console.log(
      `  would replay to: ${target.topic ?? `(unroutable: ${target.problem})`}${target.topic ? `  [via ${target.via}]` : ''}`,
    );
    console.log(`  payload    : ${preview(m.value)}`);
    console.log('');
  }
  console.log('Replay one with:  npm run dlq:replay -- --partition <p> --offset <o> --yes');
}

/** Headers for the republished copy: business headers kept, failure headers dropped, trace added. */
function buildReplayHeaders(m: DlqMessage): Record<string, string> {
  const out: Record<string, string> = {};
  for (const [name, value] of Object.entries(m.headers)) {
    if (FAILURE_HEADERS_EXACT.has(name)) continue;
    if (FAILURE_HEADER_PREFIXES.some((p) => name.startsWith(p))) continue;
    out[name] = value;
  }
  out['x-replayed-at'] = new Date().toISOString();
  out['x-replayed-from-dlq-partition'] = String(m.partition);
  out['x-replayed-from-dlq-offset'] = m.offset;
  return out;
}

async function runReplay(
  kafka: Kafka,
  selected: DlqMessage[],
  opts: { topic: string | undefined; dryRun: boolean },
): Promise<void> {
  type Plan = { message: DlqMessage; topic: ReplayableTopic; via: string };
  const plan: Plan[] = [];
  const skipped: Array<{ message: DlqMessage; problem: string }> = [];

  for (const message of selected) {
    const r = resolveTarget(message, opts.topic);
    if (r.topic) plan.push({ message, topic: r.topic, via: r.via });
    else skipped.push({ message, problem: r.problem ?? 'unroutable' });
  }

  console.log(
    `${opts.dryRun ? 'DRY RUN: ' : ''}${plan.length} message(s) to replay, ${skipped.length} skipped\n`,
  );
  for (const p of plan) {
    const f = failureSummary(p.message);
    console.log(
      `  dlq[${p.message.partition}]@${p.message.offset}  ${p.message.eventType ?? '?'}  -> ${p.topic}  ` +
        `(via ${p.via}; failed in ${f.service}: ${f.reason})`,
    );
  }
  for (const s of skipped) {
    console.log(`  dlq[${s.message.partition}]@${s.message.offset}  SKIPPED: ${s.problem}`);
  }

  if (opts.dryRun || plan.length === 0) {
    if (opts.dryRun) console.log('\nNothing published (--dry-run).');
    return;
  }

  const producer: Producer = kafka.producer({ idempotent: true, maxInFlightRequests: 1 });
  await producer.connect();
  try {
    // Preserve DLQ order within each target topic: one send per message, sequentially.
    for (const p of plan) {
      await producer.send({
        topic: p.topic,
        acks: -1,
        messages: [
          {
            key: p.message.key,
            value: p.message.value,
            headers: buildReplayHeaders(p.message),
          },
        ],
      });
    }
  } finally {
    await producer.disconnect();
  }

  console.log(`\nReplayed ${plan.length} message(s).`);
  console.log(
    'Watch the consumers (service logs / Kafka UI at http://localhost:8080). Messages that fail again return to the DLQ.',
  );
}

// ---------------------------------------------------------------------------
// CLI
// ---------------------------------------------------------------------------

function usage(): void {
  console.log(
    [
      'Usage:',
      '  tsx scripts/dlq-replay.ts inspect [--limit N] [--type T] [--grep TEXT] [--partition P] [--offset O] [--json]',
      '  tsx scripts/dlq-replay.ts replay  (--all | --offset O) [--partition P] [--type T] [--grep TEXT]',
      '                                    [--topic NAME] [--dry-run] [--yes]',
      '',
      'See the header comment of this file for the full documentation.',
    ].join('\n'),
  );
}

async function main(): Promise<void> {
  const { positionals, values } = parseArgs({
    allowPositionals: true,
    options: {
      limit: { type: 'string', default: '50' },
      type: { type: 'string' },
      grep: { type: 'string' },
      partition: { type: 'string' },
      offset: { type: 'string' },
      topic: { type: 'string' },
      all: { type: 'boolean', default: false },
      'dry-run': { type: 'boolean', default: false },
      yes: { type: 'boolean', short: 'y', default: false },
      json: { type: 'boolean', default: false },
      help: { type: 'boolean', short: 'h', default: false },
    },
  });

  const command = positionals[0];
  if (values.help || (command !== 'inspect' && command !== 'replay')) {
    usage();
    process.exitCode = values.help ? 0 : 2;
    return;
  }

  const partition = values.partition === undefined ? undefined : Number(values.partition);
  if (partition !== undefined && !Number.isInteger(partition)) {
    throw new Error(`--partition must be an integer (got "${values.partition}")`);
  }
  if (values.offset !== undefined && !/^\d+$/.test(values.offset)) {
    throw new Error(`--offset must be a non-negative integer (got "${values.offset}")`);
  }
  const filters: Filters = {
    type: values.type,
    grep: values.grep,
    partition,
    offset: values.offset,
  };

  const kafka = makeKafka();
  const all = await readAllDlq(kafka);
  const matching = applyFilters(all, filters);

  if (command === 'inspect') {
    const limit = Number(values.limit);
    if (!Number.isInteger(limit) || limit < 0)
      throw new Error(`--limit must be >= 0 (got "${values.limit}")`);
    // Newest first, because that is what you want when something just broke.
    const newestFirst = [...matching].sort((a, b) => b.timestamp.localeCompare(a.timestamp));
    const shown = limit === 0 ? newestFirst : newestFirst.slice(0, limit);
    printInspect(shown, all.length, values.json);
    return;
  }

  // ---- replay ----
  if (!values.all && values.offset === undefined) {
    throw new Error(
      'replay needs a selector: --offset <o> for one message, or --all for every matching message',
    );
  }
  if (matching.length === 0) {
    console.log(
      all.length === 0
        ? `The DLQ (${DLQ_TOPIC}) is empty; nothing to replay.`
        : 'No DLQ messages match the filters.',
    );
    return;
  }
  if (
    values.offset !== undefined &&
    partition === undefined &&
    new Set(matching.map((m) => m.partition)).size > 1
  ) {
    throw new Error('That offset exists in several partitions; add --partition <p>');
  }
  if (!values['dry-run'] && !values.yes) {
    console.log(
      `Refusing to publish without --yes. ${matching.length} message(s) match. Preview with --dry-run.`,
    );
    process.exitCode = 1;
    return;
  }

  await runReplay(kafka, matching, { topic: values.topic, dryRun: values['dry-run'] });
}

main().catch((err: unknown) => {
  console.error(`dlq-replay failed: ${(err as Error).message}`);
  process.exitCode = 1;
});
