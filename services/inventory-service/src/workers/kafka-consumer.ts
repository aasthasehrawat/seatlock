import type { Consumer, EachMessagePayload, IHeaders, Producer } from 'kafkajs';
import type { Logger } from 'pino';
import {
  AppError,
  EVENT_SCHEMAS,
  EVENT_TYPES,
  EventEnvelopeSchema,
  TOPICS,
  type EventEnvelope,
  type ReleaseReason,
} from '@seatlock/shared';
import { withTransaction } from '../db/pool.js';
import { createConsumer } from '../lib/kafka.js';
import { runWithCorrelationId } from '../lib/correlation.js';
import { confirmHoldInTx, releaseHoldInTx } from '../services/hold-lifecycle.js';
import type { ServiceDeps } from '../services/deps.js';

/**
 * ORDER / PAYMENT EVENT CONSUMER
 * ------------------------------
 * Reacts to what happened to a hold downstream:
 *
 *   OrderConfirmed | PaymentSucceeded            -> confirmHold   (ACTIVE -> CONFIRMED)
 *   OrderFailed | PaymentFailed | OrderExpired   -> releaseHold   (ACTIVE -> RELEASED/EXPIRED, stock back)
 *
 * EXACTLY-ONCE *EFFECTS* ON AT-LEAST-ONCE DELIVERY
 *   Kafka (plus our outbox relays) may deliver a message more than once. For each message we, in ONE
 *   Postgres transaction: (1) INSERT into processed_events (consumer_name, event_id) and
 *   (2) apply the business change. If the insert conflicts, we've seen this message: skip.
 *   If the business change fails, the insert rolls back too, so a retry is not mistaken for a duplicate.
 *   On top of that, confirmHold/releaseHold are themselves state-guarded and idempotent.
 *
 * FAILURE HANDLING (never block the partition, never lose a message)
 *   - Transient failure (DB down, deadlock, ...)  -> republish the ORIGINAL payload to `retry.5s`
 *     with attempt metadata in headers, then commit the offset. This service also consumes
 *     `retry.5s` (only messages tagged for itself), waits until the not-before time, and tries again.
 *   - After `maxAttempts` total attempts, or for non-retryable problems (malformed message, unknown hold,
 *     illegal state transition) -> `dlq` with the failure reason, for humans / the replay tool.
 *   - If even publishing to retry/DLQ fails, the error propagates, the offset is NOT committed, and
 *     Kafka redelivers: the message is never silently dropped.
 */

export const RETRY_HEADERS = {
  attempt: 'retry-attempt',
  consumer: 'retry-consumer',
  originalTopic: 'retry-original-topic',
  notBefore: 'retry-not-before',
  lastError: 'retry-last-error',
  firstFailedAt: 'retry-first-failed-at',
} as const;

export const DLQ_HEADERS = {
  reason: 'dlq-reason',
  error: 'dlq-error',
  consumer: 'dlq-consumer',
  originalTopic: 'dlq-original-topic',
  attempts: 'dlq-attempts',
  failedAt: 'dlq-failed-at',
} as const;

type Action =
  | { kind: 'confirm' }
  | { kind: 'release'; reason: ReleaseReason };

const ACTIONS: Partial<Record<string, Action>> = {
  [EVENT_TYPES.OrderConfirmed]: { kind: 'confirm' },
  [EVENT_TYPES.PaymentSucceeded]: { kind: 'confirm' },
  [EVENT_TYPES.PaymentFailed]: { kind: 'release', reason: 'PAYMENT_FAILED' },
  [EVENT_TYPES.OrderFailed]: { kind: 'release', reason: 'CANCELLED' },
  [EVENT_TYPES.OrderExpired]: { kind: 'release', reason: 'EXPIRED' },
};

/** A message that can never succeed no matter how often we retry it. */
export class PoisonMessageError extends Error {
  constructor(
    public readonly reason: string,
    message: string,
  ) {
    super(message);
    this.name = 'PoisonMessageError';
  }
}

export type HandleOutcome = 'processed' | 'duplicate' | 'ignored' | 'retried' | 'dead-lettered';

export interface ConsumerDeps {
  deps: ServiceDeps;
  /** Name under which processed_events records this consumer, and retry messages are tagged. */
  consumerName: string;
  producer: () => Promise<Pick<Producer, 'send'>>;
  logger: Logger;
  /** Total processing attempts (first try included) before dead-lettering. */
  maxAttempts: number;
  retryDelayMs?: number;
  /** Injectable for tests. */
  now?: () => number;
  sleep?: (ms: number) => Promise<void>;
}

export interface IncomingMessage {
  topic: string;
  key: Buffer | null;
  value: Buffer | null;
  headers?: IHeaders;
  /** Optional liveness callback invoked while waiting for a retry delay. */
  heartbeat?: () => Promise<void>;
}

function normalizeHeaders(headers: IHeaders | undefined): Record<string, string> {
  const out: Record<string, string> = {};
  for (const [k, v] of Object.entries(headers ?? {})) {
    const first = Array.isArray(v) ? v[0] : v;
    if (first !== undefined) out[k] = first.toString();
  }
  return out;
}

const defaultSleep = (ms: number): Promise<void> => new Promise((resolve) => setTimeout(resolve, ms));

/** Entry point for every message from orders.events, payments.events, retry.5s and retry.30s. */
export async function handleMessage(d: ConsumerDeps, msg: IncomingMessage): Promise<HandleOutcome> {
  const headers = normalizeHeaders(msg.headers);
  const isRetryTopic = msg.topic === TOPICS.RETRY_5S || msg.topic === TOPICS.RETRY_30S;

  if (isRetryTopic) {
    // Retry topics are shared by every service: ignore other services' retries.
    if (headers[RETRY_HEADERS.consumer] !== d.consumerName) return 'ignored';
    await waitUntilNotBefore(d, headers[RETRY_HEADERS.notBefore], msg.heartbeat);
  }

  const originalTopic = isRetryTopic ? (headers[RETRY_HEADERS.originalTopic] ?? msg.topic) : msg.topic;
  const priorFailures = isRetryTopic ? Number(headers[RETRY_HEADERS.attempt] ?? '0') || 0 : 0;

  try {
    if (!msg.value) throw new PoisonMessageError('EMPTY_MESSAGE', 'Message has no value');
    return await processRaw(d, msg.value.toString('utf8'));
  } catch (err) {
    return handleFailure(d, msg, headers, originalTopic, priorFailures, err);
  }
}

async function waitUntilNotBefore(
  d: ConsumerDeps,
  notBeforeHeader: string | undefined,
  heartbeat?: () => Promise<void>,
): Promise<void> {
  const now = d.now ?? Date.now;
  const sleep = d.sleep ?? defaultSleep;
  const notBefore = Number(notBeforeHeader);
  if (!Number.isFinite(notBefore)) return;
  // Sleep in small slices and heartbeat so the broker doesn't consider this consumer dead.
  for (let remaining = notBefore - now(); remaining > 0; remaining = notBefore - now()) {
    await sleep(Math.min(remaining, 1_000));
    await heartbeat?.();
  }
}

/** Parse + validate + apply. Throws PoisonMessageError for unfixable input. */
async function processRaw(d: ConsumerDeps, raw: string): Promise<HandleOutcome> {
  let json: unknown;
  try {
    json = JSON.parse(raw);
  } catch {
    throw new PoisonMessageError('INVALID_JSON', 'Message is not valid JSON');
  }

  const parsedEnvelope = EventEnvelopeSchema.safeParse(json);
  if (!parsedEnvelope.success) {
    throw new PoisonMessageError('INVALID_ENVELOPE', parsedEnvelope.error.message);
  }
  const envelope = parsedEnvelope.data as EventEnvelope;

  const action = ACTIONS[envelope.type];
  if (!action) return 'ignored'; // e.g. OrderCreated: not our concern

  const payloadResult = EVENT_SCHEMAS[envelope.type].safeParse(envelope.payload);
  if (!payloadResult.success) {
    throw new PoisonMessageError('INVALID_PAYLOAD', payloadResult.error.message);
  }
  const payload = payloadResult.data as { holdId: string; orderId?: string };

  return runWithCorrelationId(envelope.correlationId, () =>
    withTransaction(d.deps.pool, async (client): Promise<HandleOutcome> => {
      // Idempotency marker and business change commit (or roll back) together.
      const marker = await client.query(
        `INSERT INTO processed_events (consumer_name, event_id)
         VALUES ($1, $2)
         ON CONFLICT (consumer_name, event_id) DO NOTHING`,
        [d.consumerName, envelope.eventId],
      );
      if (marker.rowCount === 0) {
        d.logger.info({ eventId: envelope.eventId, type: envelope.type }, 'duplicate message skipped');
        return 'duplicate';
      }

      if (action.kind === 'confirm') {
        if (!payload.orderId) {
          throw new PoisonMessageError('INVALID_PAYLOAD', `${envelope.type} is missing orderId`);
        }
        const result = await confirmHoldInTx(
          client,
          d.deps,
          payload.holdId,
          payload.orderId,
          envelope.correlationId,
        );
        d.logger.info({ type: envelope.type, ...result }, 'hold confirm handled');
      } else {
        const result = await releaseHoldInTx(
          client,
          d.deps,
          payload.holdId,
          action.reason,
          envelope.correlationId,
        );
        d.logger.info(
          {
            type: envelope.type,
            holdId: result.holdId,
            released: result.released,
            currentStatus: result.currentStatus,
            offers: result.offers.length,
          },
          'hold release handled',
        );
      }
      return 'processed';
    }),
  );
}

function isNonRetryable(err: unknown): boolean {
  // AppError = a business rule said no (hold not found, illegal transition): retrying cannot help.
  return err instanceof PoisonMessageError || err instanceof AppError;
}

async function handleFailure(
  d: ConsumerDeps,
  msg: IncomingMessage,
  headers: Record<string, string>,
  originalTopic: string,
  priorFailures: number,
  err: unknown,
): Promise<HandleOutcome> {
  const now = d.now ?? Date.now;
  const errorMessage = err instanceof Error ? err.message : String(err);
  const failedAttempts = priorFailures + 1;
  const producer = await d.producer();

  const nonRetryable = isNonRetryable(err);
  if (nonRetryable || failedAttempts >= d.maxAttempts) {
    const reason =
      err instanceof PoisonMessageError
        ? err.reason
        : err instanceof AppError
          ? err.code
          : 'MAX_ATTEMPTS_EXCEEDED';
    d.logger.error(
      { err, originalTopic, attempts: failedAttempts, reason },
      'message dead-lettered',
    );
    await producer.send({
      topic: TOPICS.DLQ,
      messages: [
        {
          key: msg.key,
          value: msg.value,
          headers: {
            [DLQ_HEADERS.reason]: reason,
            [DLQ_HEADERS.error]: errorMessage.slice(0, 1_000),
            [DLQ_HEADERS.consumer]: d.consumerName,
            [DLQ_HEADERS.originalTopic]: originalTopic,
            [DLQ_HEADERS.attempts]: String(failedAttempts),
            [DLQ_HEADERS.failedAt]: new Date(now()).toISOString(),
          },
        },
      ],
    });
    return 'dead-lettered';
  }

  // Ladder: first failures go to retry.5s; later ones escalate to retry.30s before DLQ.
  // Attempt 1..N/2  -> retry.5s (short backoff)
  // Attempt N/2+1..N-1 -> retry.30s (longer backoff)
  // Attempt >= N -> DLQ (handled above)
  const escalateAfter = Math.max(1, Math.floor(d.maxAttempts / 2));
  const useSlowRetry = failedAttempts > escalateAfter || msg.topic === TOPICS.RETRY_30S;
  const retryTopic = useSlowRetry ? TOPICS.RETRY_30S : TOPICS.RETRY_5S;
  const delayMs = useSlowRetry ? 30_000 : (d.retryDelayMs ?? 5_000);

  d.logger.warn(
    {
      err,
      originalTopic,
      attempt: failedAttempts,
      maxAttempts: d.maxAttempts,
      retryTopic,
      delayMs,
    },
    'message processing failed; scheduling retry',
  );
  await producer.send({
    topic: retryTopic,
    messages: [
      {
        key: msg.key,
        value: msg.value, // ORIGINAL payload, untouched: same envelope eventId => dedupe still works
        headers: {
          [RETRY_HEADERS.attempt]: String(failedAttempts),
          [RETRY_HEADERS.consumer]: d.consumerName,
          [RETRY_HEADERS.originalTopic]: originalTopic,
          [RETRY_HEADERS.notBefore]: String(now() + delayMs),
          [RETRY_HEADERS.lastError]: errorMessage.slice(0, 1_000),
          [RETRY_HEADERS.firstFailedAt]:
            headers[RETRY_HEADERS.firstFailedAt] ?? new Date(now()).toISOString(),
        },
      },
    ],
  });
  return 'retried';
}

// ---------------------------------------------------------------------------
// Runtime wiring
// ---------------------------------------------------------------------------

export interface KafkaConsumerWorker {
  start(): void;
  stop(): Promise<void>;
}

export function createKafkaConsumerWorker(d: ConsumerDeps & { groupId?: string }): KafkaConsumerWorker {
  const consumer: Consumer = createConsumer(d.groupId ?? d.consumerName);
  let stopped = false;
  let starting: Promise<void> = Promise.resolve();

  /**
   * Keep trying until the broker is reachable, so the service boots (and serves HTTP) even if
   * Kafka is temporarily down. Once `run` is going, kafkajs handles reconnects/rebalances itself.
   */
  async function connectLoop(): Promise<void> {
    while (!stopped) {
      try {
        await consumer.connect();
        await consumer.subscribe({
          topics: [TOPICS.ORDERS, TOPICS.PAYMENTS, TOPICS.RETRY_5S, TOPICS.RETRY_30S],
          fromBeginning: false,
        });
        await consumer.run({
          eachMessage: async ({ topic, message, heartbeat }: EachMessagePayload) => {
            await handleMessage(d, {
              topic,
              key: message.key,
              value: message.value,
              headers: message.headers,
              heartbeat,
            });
          },
        });
        d.logger.info('kafka consumer running');
        return;
      } catch (err) {
        d.logger.error({ err }, 'kafka consumer failed to start; retrying in 5s');
        await (d.sleep ?? defaultSleep)(5_000);
      }
    }
  }

  return {
    start() {
      starting = connectLoop();
    },
    async stop() {
      stopped = true;
      await starting.catch(() => undefined);
      await consumer.disconnect().catch((err: Error) =>
        d.logger.warn({ err: err.message }, 'consumer disconnect failed'),
      );
    },
  };
}
