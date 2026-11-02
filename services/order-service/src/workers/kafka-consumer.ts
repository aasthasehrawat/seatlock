import type { Consumer, EachMessagePayload, IHeaders, Kafka, Producer } from 'kafkajs';
import { TOPICS } from '@seatlock/shared';
import { NonRetryableError } from '../lib/errors.js';
import type { Logger } from '../lib/logger.js';
import { sleep } from '../lib/sleep.js';
import { parseMessage, type MessageDispatcher } from './message-handler.js';
import { HEADER, describeError, getHeader } from './retry-headers.js';

export interface KafkaConsumerOptions {
  kafka: Kafka;
  producer: Producer;
  dispatch: MessageDispatcher;
  logger: Logger;
  serviceName: string;
  groupId: string;
  fromBeginning: boolean;
  retryDelayMs: number;
  maxRetryAttempts: number;
  /** Called if a consumer crashes and KafkaJS will not restart it. */
  onFatal?: (err: Error) => void;
}

/** Everything needed to re-park a failed message and to describe where it came from. */
interface Origin {
  topic: string;
  partition: number;
  offset: string;
  key: Buffer | null;
  value: Buffer | null;
  headers: IHeaders | undefined;
}

/**
 * Two KafkaJS consumers, one failure policy.
 *
 *   inventory.events + payments.events --(main consumer)--> OrderService
 *                  |
 *        processing failed?
 *         |- NonRetryable / poison ----------------------------> dlq
 *         `- transient ----> retry.5s --(retry consumer, waits)--> OrderService
 *                                         failed again?  attempts left -> retry.5s
 *                                                        exhausted      -> dlq
 *
 * Why a retry topic instead of retrying in place? An in-place retry loop blocks the partition
 * ("head-of-line blocking"): one stuck message would stall every order behind it. Parking the
 * message lets the main consumer keep moving. The trade-off is that a retried message can be
 * processed after later messages for the same order; that is safe here because the state machine
 * and processed_events make every handler idempotent and order-tolerant.
 *
 * Offsets are committed by KafkaJS only after the handler returns. If handing a message to
 * retry/DLQ itself fails, the error propagates, the offset stays uncommitted and the message is
 * redelivered: failure handling can delay a message but never lose it.
 */
export class OrderEventConsumer {
  private readonly main: Consumer;
  private readonly retry: Consumer;
  private readonly log: Logger;
  private running = false;

  constructor(private readonly opts: KafkaConsumerOptions) {
    this.log = opts.logger.child({ component: 'kafka-consumer' });
    this.main = opts.kafka.consumer({ groupId: opts.groupId, allowAutoTopicCreation: true });
    this.retry = opts.kafka.consumer({
      groupId: `${opts.groupId}-retry`,
      allowAutoTopicCreation: true,
    });
  }

  isRunning(): boolean {
    return this.running;
  }

  async start(): Promise<void> {
    this.watchForCrash(this.main, 'main');
    this.watchForCrash(this.retry, 'retry');

    await this.main.connect();
    await this.retry.connect();

    await this.main.subscribe({
      topics: [TOPICS.INVENTORY, TOPICS.PAYMENTS],
      fromBeginning: this.opts.fromBeginning,
    });
    // Retry messages are short-lived: never replay old ones for a brand-new group.
    await this.retry.subscribe({ topics: [TOPICS.RETRY_5S], fromBeginning: false });

    await this.main.run({
      eachMessage: (payload) => this.onMainMessage(payload),
    });
    await this.retry.run({
      eachMessage: (payload) => this.onRetryMessage(payload),
    });

    this.running = true;
    this.log.info(
      { groupId: this.opts.groupId, topics: [TOPICS.INVENTORY, TOPICS.PAYMENTS, TOPICS.RETRY_5S] },
      'kafka consumers running',
    );
  }

  async stop(): Promise<void> {
    this.running = false;
    await Promise.allSettled([this.main.disconnect(), this.retry.disconnect()]);
    this.log.info('kafka consumers stopped');
  }

  // -------------------------------------------------------------------------
  // Main path
  // -------------------------------------------------------------------------

  private async onMainMessage({ topic, partition, message }: EachMessagePayload): Promise<void> {
    const origin: Origin = {
      topic,
      partition,
      offset: message.offset,
      key: message.key,
      value: message.value,
      headers: message.headers,
    };

    try {
      await this.process(origin);
    } catch (err) {
      await this.handleFailure(err, origin, 0);
    }
  }

  // -------------------------------------------------------------------------
  // Retry path
  // -------------------------------------------------------------------------

  private async onRetryMessage({ message, heartbeat }: EachMessagePayload): Promise<void> {
    // retry.5s is shared: skip messages that another service parked.
    if (getHeader(message.headers, HEADER.RETRY_SERVICE) !== this.opts.serviceName) return;

    const attempt = Number(getHeader(message.headers, HEADER.RETRY_ATTEMPT) ?? '1');
    const retryAt = Number(getHeader(message.headers, HEADER.RETRY_AT) ?? '0');

    const origin: Origin = {
      topic: getHeader(message.headers, HEADER.ORIGINAL_TOPIC) ?? 'unknown',
      partition: Number(getHeader(message.headers, HEADER.ORIGINAL_PARTITION) ?? '-1'),
      offset: getHeader(message.headers, HEADER.ORIGINAL_OFFSET) ?? '-1',
      key: message.key,
      value: message.value,
      headers: message.headers,
    };

    // The delay: hold this partition until the message is due. Capped at the configured delay so
    // a bad header cannot park the consumer, and chunked so heartbeats keep the session alive.
    await this.waitUntil(retryAt, heartbeat);

    try {
      await this.process(origin);
      this.log.info({ topic: origin.topic, attempt }, 'retried message succeeded');
    } catch (err) {
      await this.handleFailure(err, origin, attempt);
    }
  }

  private async waitUntil(epochMs: number, heartbeat: () => Promise<void>): Promise<void> {
    const deadline = Math.min(epochMs, Date.now() + this.opts.retryDelayMs);
    for (let remaining = deadline - Date.now(); remaining > 0; remaining = deadline - Date.now()) {
      await sleep(Math.min(remaining, 1000));
      await heartbeat();
    }
  }

  // -------------------------------------------------------------------------
  // Shared processing + failure policy
  // -------------------------------------------------------------------------

  private async process(origin: Origin): Promise<void> {
    const message = parseMessage(origin.value);
    const result = await this.opts.dispatch(message);

    this.log.debug(
      {
        topic: origin.topic,
        partition: origin.partition,
        offset: origin.offset,
        type: message.type,
        eventId: message.meta.eventId,
        correlationId: message.meta.correlationId,
        outcome: result.outcome,
        reason: result.reason,
      },
      'message processed',
    );
  }

  /**
   * `retriesDone` is how many retries have already been attempted (0 for the main consumer).
   * Retry while attempts remain, otherwise dead-letter.
   */
  private async handleFailure(err: unknown, origin: Origin, retriesDone: number): Promise<void> {
    const logFields = {
      err,
      topic: origin.topic,
      partition: origin.partition,
      offset: origin.offset,
      retriesDone,
    };

    if (err instanceof NonRetryableError) {
      this.log.error(logFields, 'non-retryable failure: sending to DLQ');
      await this.sendToDlq(origin, err);
      return;
    }

    if (retriesDone >= this.opts.maxRetryAttempts) {
      this.log.error(logFields, 'retries exhausted: sending to DLQ');
      await this.sendToDlq(origin, err);
      return;
    }

    this.log.warn(logFields, 'processing failed: scheduling retry');
    await this.sendToRetry(origin, err, retriesDone + 1);
  }

  private async sendToRetry(origin: Origin, err: unknown, attempt: number): Promise<void> {
    await this.opts.producer.send({
      topic: TOPICS.RETRY_5S,
      messages: [
        {
          key: origin.key,
          value: origin.value,
          headers: {
            ...this.passthroughHeaders(origin),
            [HEADER.RETRY_SERVICE]: this.opts.serviceName,
            [HEADER.RETRY_ATTEMPT]: String(attempt),
            [HEADER.RETRY_AT]: String(Date.now() + this.opts.retryDelayMs),
            [HEADER.ERROR]: describeError(err),
          },
        },
      ],
    });
  }

  private async sendToDlq(origin: Origin, err: unknown): Promise<void> {
    await this.opts.producer.send({
      topic: TOPICS.DLQ,
      messages: [
        {
          key: origin.key,
          value: origin.value,
          headers: {
            ...this.passthroughHeaders(origin),
            [HEADER.FAILED_SERVICE]: this.opts.serviceName,
            [HEADER.FAILED_AT]: new Date().toISOString(),
            [HEADER.ERROR]: describeError(err),
          },
        },
      ],
    });
  }

  /** Original provenance headers, preserved across hops so the DLQ shows where a message began. */
  private passthroughHeaders(origin: Origin): Record<string, string> {
    return {
      [HEADER.ORIGINAL_TOPIC]: origin.topic,
      [HEADER.ORIGINAL_PARTITION]: String(origin.partition),
      [HEADER.ORIGINAL_OFFSET]: origin.offset,
      // Carry the business headers (event-type, correlation-id, ...) through untouched.
      ...this.businessHeaders(origin.headers),
    };
  }

  private businessHeaders(headers: IHeaders | undefined): Record<string, string> {
    const reserved = new Set<string>(Object.values(HEADER));
    const out: Record<string, string> = {};
    for (const name of Object.keys(headers ?? {})) {
      if (reserved.has(name)) continue;
      const value = getHeader(headers, name);
      if (value !== undefined) out[name] = value;
    }
    return out;
  }

  private watchForCrash(consumer: Consumer, name: string): void {
    consumer.on(consumer.events.CRASH, ({ payload }) => {
      this.log.error({ err: payload.error, restart: payload.restart, consumer: name }, 'consumer crashed');
      if (!payload.restart) {
        this.running = false;
        this.opts.onFatal?.(payload.error);
      }
    });
  }
}