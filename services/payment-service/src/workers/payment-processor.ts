import { v4 as uuidv4 } from 'uuid';
import {
  EVENT_TYPES,
  InvalidStateTransitionError,
  NotFoundError,
  PaymentRequestedSchema,
  TOPICS,
  createEnvelope,
  createId,
  type EventEnvelope,
  type PaymentFailedPayload,
  type PaymentRequestedPayload,
  type PaymentSucceededPayload,
} from '@seatlock/shared';
import { decodeEnvelope } from '../lib/kafka.js';
import type { Logger } from '../lib/logger.js';
import type { OutboxMessage, Payment, PaymentRepository, PaymentResult, PaymentTx } from '../payments/types.js';

export const FAILURE_REASONS = [
  'CARD_DECLINED',
  'INSUFFICIENT_FUNDS',
  'PROCESSOR_TIMEOUT',
  'FRAUD_SUSPECTED',
] as const;

export interface ProcessorDeps {
  repo: PaymentRepository;
  logger: Logger;
  /** Name stamped into the `producer` field of outgoing envelopes. */
  producerName: string;
  /** Probability in [0, 1] that a payment fails. */
  failureRate: number;
  /** Simulated processor latency. */
  latencyMs: number;
  /** Injectable for deterministic tests. Must return a value in [0, 1). */
  random?: () => number;
  /** Injectable so tests don't actually wait. */
  sleep?: (ms: number) => Promise<void>;
}

export type ProcessResult =
  /** This exact Kafka event was already handled; nothing was done. */
  | { kind: 'duplicate_event' }
  /** A payment for this order already existed; no new charge was made. */
  | { kind: 'duplicate_request'; payment: Payment; republished: boolean }
  /** The payment was charged (or failed) by this call. */
  | { kind: 'processed'; payment: Payment };

const defaultSleep = (ms: number): Promise<void> => new Promise((resolve) => setTimeout(resolve, ms));

/** Turn a payment's terminal state into the outbox message announcing it. */
export function buildResultMessage(payment: Payment, correlationId: string, producer: string): OutboxMessage {
  if (payment.status === 'SUCCEEDED') {
    const payload: PaymentSucceededPayload = {
      paymentId: payment.id,
      orderId: payment.orderId,
      holdId: payment.holdId,
      amountCents: payment.amountCents,
      providerRef: payment.providerRef ?? `mock_${payment.id}`,
    };
    const envelope = createEnvelope({ type: EVENT_TYPES.PaymentSucceeded, payload, producer, correlationId });
    return { eventId: envelope.eventId, topic: TOPICS.PAYMENTS, key: payment.orderId, envelope };
  }

  if (payment.status === 'FAILED') {
    const payload: PaymentFailedPayload = {
      paymentId: payment.id,
      orderId: payment.orderId,
      holdId: payment.holdId,
      amountCents: payment.amountCents,
      reason: payment.failureReason ?? 'UNKNOWN',
    };
    const envelope = createEnvelope({ type: EVENT_TYPES.PaymentFailed, payload, producer, correlationId });
    return { eventId: envelope.eventId, topic: TOPICS.PAYMENTS, key: payment.orderId, envelope };
  }

  throw new Error(`Cannot build result message for ${payment.status} payment ${payment.id}`);
}

/** Record a terminal result on the payment and enqueue the matching outbox event. */
async function applyResult(
  tx: PaymentTx,
  payment: Payment,
  result: PaymentResult,
  correlationId: string,
  producer: string,
): Promise<Payment> {
  const updated = await tx.recordResult(payment.id, result);
  await tx.enqueueOutbox(buildResultMessage(updated, correlationId, producer));
  return updated;
}

/** Decide the outcome of one charge attempt. */
function decideOutcome(deps: ProcessorDeps): PaymentResult {
  const random = deps.random ?? Math.random;
  if (random() < deps.failureRate) {
    const index = Math.min(FAILURE_REASONS.length - 1, Math.floor(random() * FAILURE_REASONS.length));
    return { status: 'FAILED', providerRef: null, failureReason: FAILURE_REASONS[index] ?? 'UNKNOWN' };
  }
  return { status: 'SUCCEEDED', providerRef: `mock_${uuidv4()}`, failureReason: null };
}

/**
 * Handle one PaymentRequested event.
 *
 * Exactly-once *effect* is achieved in three layers:
 *   1. processed_events: the same Kafka event is never handled twice.
 *   2. UNIQUE(order_id): a second request for the same order finds the existing payment.
 *   3. Row lock + status check: if two workers race on one PENDING payment, only the
 *      first to take the lock writes a result; the other sees a terminal status and stops.
 *
 * The simulated latency happens between two short transactions so we never hold
 * a DB connection (or row lock) while "talking to the payment provider".
 */
export async function processPaymentRequested(
  deps: ProcessorDeps,
  envelope: EventEnvelope<PaymentRequestedPayload>,
): Promise<ProcessResult> {
  const { repo, logger } = deps;
  const request = envelope.payload;
  const log = logger.child({ eventId: envelope.eventId, orderId: request.orderId, correlationId: envelope.correlationId });

  // Layer 1: cheap pre-check (the authoritative check is markEventProcessed in the tx).
  if (await repo.isEventProcessed(envelope.eventId)) {
    log.info('PaymentRequested already processed; skipping');
    return { kind: 'duplicate_event' };
  }

  // Transaction 1: make sure a payment row exists, and see if it's already settled.
  const existing = await repo.withTransaction(async (tx) => {
    await tx.insertPendingIfAbsent({
      id: request.paymentId,
      orderId: request.orderId,
      holdId: request.holdId,
      userId: request.userId,
      amountCents: request.amountCents,
      currency: request.currency,
    });

    const payment = await tx.findByOrderId(request.orderId);
    if (!payment) {
      // The insert was skipped but no payment exists for this order: paymentId collided with another order's payment.
      throw new Error(`paymentId ${request.paymentId} is already used by a different order`);
    }

    if (payment.status === 'PENDING') {
      return null;
    }

    // Layer 2: already settled. Don't charge again; re-announce the result so a caller that missed it can recover.
    const firstTime = await tx.markEventProcessed(envelope.eventId, envelope.type);
    if (!firstTime) {
      return 'duplicate' as const;
    }
    await tx.enqueueOutbox(buildResultMessage(payment, envelope.correlationId, deps.producerName));
    return payment;
  });

  if (existing === 'duplicate') {
    return { kind: 'duplicate_event' };
  }
  if (existing) {
    log.info({ paymentId: existing.id, status: existing.status }, 'Payment already exists for order; republished result');
    return { kind: 'duplicate_request', payment: existing, republished: true };
  }

  // Simulate talking to the payment provider.
  if (deps.latencyMs > 0) {
    await (deps.sleep ?? defaultSleep)(deps.latencyMs);
  }
  const outcome = decideOutcome(deps);

  // Transaction 2: settle the payment, unless someone else already did.
  const settled = await repo.withTransaction(async (tx) => {
    const locked = await tx.lockById(request.paymentId);
    const current = locked ?? (await tx.findByOrderId(request.orderId));
    if (!current) {
      throw new Error(`Payment for order ${request.orderId} vanished before settlement`);
    }

    const firstTime = await tx.markEventProcessed(envelope.eventId, envelope.type);
    if (!firstTime) {
      return { duplicateEvent: true as const };
    }

    if (current.status !== 'PENDING') {
      // Layer 3: lost the race; the winner already published the result.
      return { duplicateEvent: false as const, payment: current, won: false };
    }

    const payment = await applyResult(tx, current, outcome, envelope.correlationId, deps.producerName);
    return { duplicateEvent: false as const, payment, won: true };
  });

  if (settled.duplicateEvent) {
    return { kind: 'duplicate_event' };
  }
  if (!settled.won) {
    log.info({ paymentId: settled.payment.id }, 'Payment was settled concurrently; not charging again');
    return { kind: 'duplicate_request', payment: settled.payment, republished: false };
  }

  log.info(
    { paymentId: settled.payment.id, status: settled.payment.status, reason: settled.payment.failureReason },
    'Payment processed',
  );
  return { kind: 'processed', payment: settled.payment };
}

export interface ForceResultOptions {
  repo: PaymentRepository;
  producerName: string;
  paymentId: string;
  outcome: 'succeed' | 'fail';
  reason?: string | undefined;
  /** Allow overriding a payment that is already SUCCEEDED/FAILED. */
  force?: boolean | undefined;
  correlationId?: string | undefined;
}

/**
 * Debug helper behind POST /payments/:id/simulate: force a payment to succeed or fail
 * and publish the matching event. Without `force`, only PENDING payments may be settled.
 */
export async function forcePaymentResult(options: ForceResultOptions): Promise<Payment> {
  const target = options.outcome === 'succeed' ? 'SUCCEEDED' : 'FAILED';

  return options.repo.withTransaction(async (tx) => {
    const payment = await tx.lockById(options.paymentId);
    if (!payment) {
      throw new NotFoundError('Payment', options.paymentId);
    }
    if (payment.status !== 'PENDING' && !options.force) {
      throw new InvalidStateTransitionError('payment', payment.status, target);
    }

    const result: PaymentResult =
      target === 'SUCCEEDED'
        ? { status: 'SUCCEEDED', providerRef: `mock_${uuidv4()}`, failureReason: null }
        : { status: 'FAILED', providerRef: null, failureReason: options.reason ?? 'SIMULATED_FAILURE' };

    return applyResult(tx, payment, result, options.correlationId ?? createId(), options.producerName);
  });
}

export type MessageOutcome =
  | { kind: 'ignored'; type: string }
  | { kind: 'invalid'; reason: string }
  | { kind: 'handled'; result: ProcessResult };

/**
 * Decode and dispatch one raw Kafka message. The payments topic also carries our own
 * PaymentSucceeded/PaymentFailed events, which are ignored here.
 */
export async function handlePaymentMessage(
  deps: ProcessorDeps,
  value: Buffer | string | null | undefined,
): Promise<MessageOutcome> {
  const decoded = decodeEnvelope(value);
  if (!decoded.ok) {
    return { kind: 'invalid', reason: decoded.reason };
  }

  const { envelope } = decoded;
  if (envelope.type !== EVENT_TYPES.PaymentRequested) {
    return { kind: 'ignored', type: envelope.type };
  }

  const payload = PaymentRequestedSchema.safeParse(envelope.payload);
  if (!payload.success) {
    return {
      kind: 'invalid',
      reason: `Invalid PaymentRequested payload: ${payload.error.issues.map((i) => `${i.path.join('.')} ${i.message}`).join('; ')}`,
    };
  }

  const result = await processPaymentRequested(deps, { ...envelope, payload: payload.data });
  return { kind: 'handled', result };
}
