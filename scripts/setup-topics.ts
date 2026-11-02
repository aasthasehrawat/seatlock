/**
 * setup-topics.ts: create every Kafka topic SeatLock uses, with explicit partition counts.
 *
 * WHY THIS EXISTS
 *   The broker in docker-compose.yml has KAFKA_AUTO_CREATE_TOPICS_ENABLE=true, and the services
 *   also try to create topics at boot, but auto-created topics get the broker default of ONE
 *   partition. One partition means one consumer per group and no parallelism, which defeats the
 *   point of keying events by concert / order id. Run this once, BEFORE starting the services, so
 *   the topics exist with the intended layout. (kafkajs createTopics never changes an existing
 *   topic, so later service-side "ensure" calls are harmless no-ops.)
 *
 * USAGE
 *   npm run setup:topics                       # uses KAFKA_BROKERS or localhost:9092
 *   tsx scripts/setup-topics.ts                # same thing, without npm
 *   tsx scripts/setup-topics.ts --dry-run      # print the plan, change nothing
 *   tsx scripts/setup-topics.ts --list         # show current topics, partitions and offsets
 *
 * ENVIRONMENT
 *   KAFKA_BROKERS             comma-separated broker list            (default localhost:9092)
 *   KAFKA_REPLICATION_FACTOR  replication factor for new topics      (default 1; the compose
 *                                                                     stack has a single broker)
 *   KAFKA_WAIT_SECONDS        how long to wait for the broker        (default 60)
 *
 * IDEMPOTENT: safe to run any number of times. Existing topics are reported, never modified. If an
 * existing topic has fewer partitions than planned (for example it was auto-created with 1), a
 * warning tells you how to fix it; the script will not silently change partition counts because
 * adding partitions changes key -> partition mapping and therefore per-key ordering.
 */

import { Kafka, logLevel, type ITopicConfig } from 'kafkajs';
import { parseArgs } from 'node:util';

interface TopicPlan {
  topic: string;
  partitions: number;
  purpose: string;
  configEntries?: Array<{ name: string; value: string }>;
}

const DAY_MS = 24 * 60 * 60 * 1000;

/**
 * Partition rationale:
 *  - inventory/orders/payments: 6. Keyed by concert id (inventory) or order id (orders/payments),
 *    so ordering is preserved per key while different keys spread across partitions. This matches
 *    what inventory-service creates for itself at boot.
 *  - notifications: 3. Lower volume, and nothing needs strict ordering across keys.
 *  - retry.*: 3. Short-lived delay queues shared by all services.
 *  - dlq: 1. Low volume, and a single partition gives a simple, totally ordered log for humans.
 */
export const TOPIC_PLAN: readonly TopicPlan[] = [
  {
    topic: 'inventory.events',
    partitions: 6,
    purpose:
      'Facts from inventory-service (reserved, released, confirmed, hold expired, waitlist offered)',
  },
  {
    topic: 'orders.events',
    partitions: 6,
    purpose: 'Facts from order-service (order created / confirmed / failed / expired)',
  },
  {
    topic: 'payments.events',
    partitions: 6,
    purpose:
      'PaymentRequested command from order-service plus PaymentSucceeded / PaymentFailed from payment-service',
  },
  {
    topic: 'notifications.events',
    partitions: 3,
    purpose: 'Reserved for NotificationRequested events (no service produces or consumes it yet)',
  },
  {
    topic: 'retry.5s',
    partitions: 3,
    purpose: 'Delay queue: messages that failed transiently, retried after ~5 seconds',
    configEntries: [{ name: 'retention.ms', value: String(DAY_MS) }],
  },
  {
    topic: 'retry.30s',
    partitions: 3,
    purpose: 'Second-tier delay queue: inventory-service escalates here after retry.5s exhaustion',
    configEntries: [{ name: 'retention.ms', value: String(DAY_MS) }],
  },
  {
    topic: 'dlq',
    partitions: 1,
    purpose: 'Dead-letter queue: poison or exhausted messages awaiting human inspection / replay',
    // Keep failures for 14 days so there is time to investigate and replay.
    configEntries: [{ name: 'retention.ms', value: String(14 * DAY_MS) }],
  },
];

const { values: args } = parseArgs({
  options: {
    'dry-run': { type: 'boolean', default: false },
    list: { type: 'boolean', default: false },
    help: { type: 'boolean', short: 'h', default: false },
  },
  allowPositionals: false,
});

if (args.help) {
  console.log(
    'Usage: tsx scripts/setup-topics.ts [--dry-run] [--list]\nSee the header comment of this file for details.',
  );
  process.exit(0);
}

const brokers = (process.env.KAFKA_BROKERS ?? 'localhost:9092')
  .split(',')
  .map((b) => b.trim())
  .filter(Boolean);
const replicationFactor = Number(process.env.KAFKA_REPLICATION_FACTOR ?? '1');
const waitSeconds = Number(process.env.KAFKA_WAIT_SECONDS ?? '60');

if (!Number.isInteger(replicationFactor) || replicationFactor < 1) {
  console.error(
    `KAFKA_REPLICATION_FACTOR must be a positive integer (got "${process.env.KAFKA_REPLICATION_FACTOR}")`,
  );
  process.exit(2);
}

const kafka = new Kafka({
  clientId: 'seatlock-setup-topics',
  brokers,
  logLevel: logLevel.NOTHING,
  retry: { retries: 2, initialRetryTime: 300 },
});
const admin = kafka.admin();

const sleep = (ms: number): Promise<void> => new Promise((resolve) => setTimeout(resolve, ms));

/** The broker container can take ~20s to become ready after `docker compose up -d`. */
async function connectWithWait(): Promise<void> {
  const deadline = Date.now() + waitSeconds * 1000;
  let attempt = 0;
  for (;;) {
    attempt += 1;
    try {
      await admin.connect();
      await admin.listTopics(); // proves the broker answers, not just that the socket opened
      return;
    } catch (err) {
      await admin.disconnect().catch(() => undefined);
      if (Date.now() >= deadline) {
        throw new Error(
          `Kafka at ${brokers.join(',')} did not become ready within ${waitSeconds}s: ${(err as Error).message}\n` +
            'Is it running? Try: docker compose up -d kafka && docker compose ps',
        );
      }
      console.log(`  waiting for Kafka (attempt ${attempt}) ...`);
      await sleep(2000);
    }
  }
}

async function listCurrent(): Promise<void> {
  const topics = (await admin.listTopics()).filter((t) => !t.startsWith('__')).sort();
  if (topics.length === 0) {
    console.log('No topics yet.');
    return;
  }
  console.log('topic'.padEnd(24) + 'partitions  messages(high-low, summed)');
  for (const topic of topics) {
    const offsets = await admin.fetchTopicOffsets(topic);
    const total = offsets.reduce((sum, o) => sum + (Number(o.high) - Number(o.low)), 0);
    console.log(topic.padEnd(24) + String(offsets.length).padEnd(12) + String(total));
  }
}

async function main(): Promise<void> {
  console.log(`Kafka brokers: ${brokers.join(', ')}`);

  if (args['dry-run']) {
    console.log(
      '\nDRY RUN: would ensure these topics (replication factor ' + replicationFactor + '):\n',
    );
    for (const t of TOPIC_PLAN) {
      console.log(`  ${t.topic.padEnd(22)} partitions=${t.partitions}  ${t.purpose}`);
    }
    return;
  }

  await connectWithWait();

  if (args.list) {
    await listCurrent();
    return;
  }

  const existing = new Set(await admin.listTopics());

  const toCreate: ITopicConfig[] = TOPIC_PLAN.filter((t) => !existing.has(t.topic)).map((t) => ({
    topic: t.topic,
    numPartitions: t.partitions,
    replicationFactor,
    ...(t.configEntries ? { configEntries: t.configEntries } : {}),
  }));

  if (toCreate.length > 0) {
    // createTopics resolves to false only when *every* requested topic already existed; since we
    // pre-filtered, that would indicate a race with another creator, which is harmless.
    await admin.createTopics({ waitForLeaders: true, topics: toCreate });
  }

  // Report the final state, and flag any existing topic whose layout differs from the plan.
  const metadata = await admin.fetchTopicMetadata({ topics: TOPIC_PLAN.map((t) => t.topic) });
  const actual = new Map(metadata.topics.map((t) => [t.name, t.partitions.length]));
  const warnings: string[] = [];

  console.log('');
  for (const t of TOPIC_PLAN) {
    const have = actual.get(t.topic);
    const wasExisting = existing.has(t.topic);
    const status = wasExisting ? 'exists ' : 'CREATED';
    console.log(
      `  [${status}] ${t.topic.padEnd(22)} partitions=${have ?? '?'} (planned ${t.partitions})`,
    );
    if (have !== undefined && have < t.partitions) {
      warnings.push(
        `${t.topic} has ${have} partition(s) but the plan is ${t.partitions}. It was probably auto-created. ` +
          `In a dev environment the simplest fix is to delete it and re-run this script:\n` +
          `      docker exec seatlock-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --delete --topic ${t.topic}`,
      );
    }
  }

  if (warnings.length > 0) {
    console.log('\nWarnings:');
    for (const w of warnings) console.log(`  - ${w}`);
  }

  console.log(
    `\nDone. ${toCreate.length} created, ${TOPIC_PLAN.length - toCreate.length} already existed. ` +
      'Inspect with: npm run setup:topics -- --list   (or Kafka UI at http://localhost:8080)',
  );
}

main()
  .catch((err: unknown) => {
    console.error(`setup-topics failed: ${(err as Error).message}`);
    process.exitCode = 1;
  })
  .finally(() => admin.disconnect().catch(() => undefined));
