import { describe, expect, it, vi } from 'vitest';
import {
  EVENT_TYPES,
  PaymentFailedSchema,
  PaymentSucceededSchema,
  TOPICS,
  createEnvelope,
  createId,
  type EventEnvelope,
  type PaymentRequestedPayload,
} from '@seatlock/shared';
import { createLogger } from '../lib/logger.js';
import { InMemoryPaymentRepository } from '../test-support/in-memory-repository.js';
import {
  FAILURE_REASONS,
  forcePaymentResult,
  handlePaymentMessage,
  processPaymentRequested,
  type ProcessorDeps,
} from './payment-processor.js';

const logger = createLogger({ level: 'silent', name: 'test' });

function makeRequest(overrides: Partial<PaymentRequestedPayload> = {}): PaymentRequestedPayload {
  return {
    paymentId: createId(),
    orderId: createId(),
    holdId: createId(),
    userId: createId(),
    amountCents: 12_500,
    currency: 'USD',
    ...overrides,
  };
}

function makeEnvelope(payload: PaymentRequestedPayload): EventEnvelope<PaymentRequestedPayload> {
  return createEnvelope({
    type: EVENT_TYPES.PaymentRequested,
    payload,
    producer: 'order-service',
    correlationId: 'corr-1',
  });
}

function setup(overrides: Partial<ProcessorDeps> = {}) {
  const repo = new InMemoryPaymentRepository();
  const sleep = vi.fn(async () => undefined);
  const deps: ProcessorDeps = {
    repo,
    logger,
    producerName: 'payment-service',
    failureRate: 0.2,
    latencyMs: 500,
    random: () => 0.9, // above failure rate => success
    sleep,
    ...overrides,
  };
  return { repo, deps, sleep };
}

describe('processPaymentRequested', () => {
  it('succeeds: stores SUCCEEDED and enqueues PaymentSucceeded', async () => {
    const { repo, deps, sleep } = setup();
    const request = makeRequest();
    const envelope = makeEnvelope(request);

    const result = await processPaymentRequested(deps, envelope);

    expect(result.kind).toBe('processed');
    const payment = repo.payments.get(request.paymentId)!;
    expect(payment.status).toBe('SUCCEEDED');
    expect(payment.providerRef).toMatch(/^mock_/);
    expect(payment.failureReason).toBeNull();
    expect(payment.attemptCount).toBe(1);
    expect(payment.amountCents).toBe(12_500);

    expect(sleep).toHaveBeenCalledWith(500);
    expect(repo.processedEvents.has(envelope.eventId)).toBe(true);

    expect(repo.outbox).toHaveLength(1);
    const message = repo.outbox[0]!;
    expect(message.topic).toBe(TOPICS.PAYMENTS);
    expect(message.key).toBe(request.orderId);
    expect(message.envelope.type).toBe(EVENT_TYPES.PaymentSucceeded);
    expect(message.envelope.correlationId).toBe('corr-1');
    expect(message.envelope.producer).toBe('payment-service');
    expect(message.eventId).toBe(message.envelope.eventId);
    expect(PaymentSucceededSchema.parse(message.envelope.payload)).toMatchObject({
      paymentId: request.paymentId,
      orderId: request.orderId,
      holdId: request.holdId,
      amountCents: 12_500,
      providerRef: payment.providerRef,
    });
  });

  it('fails: stores FAILED with a reason and enqueues PaymentFailed', async () => {
    const { repo, deps } = setup({ random: () => 0.05 }); // below failure rate => failure
    const request = makeRequest();

    const result = await processPaymentRequested(deps, makeEnvelope(request));

    expect(result.kind).toBe('processed');
    const payment = repo.payments.get(request.paymentId)!;
    expect(payment.status).toBe('FAILED');
    expect(payment.providerRef).toBeNull();
    expect(FAILURE_REASONS).toContain(payment.failureReason);
    expect(payment.attemptCount).toBe(1);

    expect(repo.outbox).toHaveLength(1);
    const envelope = repo.outbox[0]!.envelope;
    expect(envelope.type).toBe(EVENT_TYPES.PaymentFailed);
    expect(PaymentFailedSchema.parse(envelope.payload)).toMatchObject({
      paymentId: request.paymentId,
      orderId: request.orderId,
      reason: payment.failureReason,
    });
  });

  it('never fails when failureRate is 0 and always fails when it is 1', async () => {
    const never = setup({ failureRate: 0, random: () => 0 });
    const neverReq = makeRequest();
    await processPaymentRequested(never.deps, makeEnvelope(neverReq));
    expect(never.repo.payments.get(neverReq.paymentId)!.status).toBe('SUCCEEDED');

    const always = setup({ failureRate: 1, random: () => 0.999999 });
    const alwaysReq = makeRequest();
    await processPaymentRequested(always.deps, makeEnvelope(alwaysReq));
    expect(always.repo.payments.get(alwaysReq.paymentId)!.status).toBe('FAILED');
  });

  it('skips the sleep entirely when latency is 0', async () => {
    const { deps, sleep } = setup({ latencyMs: 0 });
    await processPaymentRequested(deps, makeEnvelope(makeRequest()));
    expect(sleep).not.toHaveBeenCalled();
  });

  it('ignores a redelivery of the exact same event', async () => {
    const { repo, deps, sleep } = setup();
    const request = makeRequest();
    const envelope = makeEnvelope(request);

    await processPaymentRequested(deps, envelope);
    sleep.mockClear();
    const second = await processPaymentRequested(deps, envelope);

    expect(second.kind).toBe('duplicate_event');
    expect(sleep).not.toHaveBeenCalled();
    expect(repo.payments.get(request.paymentId)!.attemptCount).toBe(1);
    expect(repo.outbox).toHaveLength(1);
  });

  it('does not double-charge when a second event requests payment for the same order', async () => {
    const { repo, deps, sleep } = setup();
    const request = makeRequest();

    await processPaymentRequested(deps, makeEnvelope(request));
    sleep.mockClear();

    // Different eventId and even a different paymentId, but the same order.
    const retry = await processPaymentRequested(deps, makeEnvelope({ ...request, paymentId: createId() }));

    expect(retry.kind).toBe('duplicate_request');
    if (retry.kind === 'duplicate_request') {
      expect(retry.republished).toBe(true);
      expect(retry.payment.id).toBe(request.paymentId);
      expect(retry.payment.status).toBe('SUCCEEDED');
    }
    expect(sleep).not.toHaveBeenCalled();
    expect(repo.payments.size).toBe(1);
    expect(repo.payments.get(request.paymentId)!.attemptCount).toBe(1);

    // The result is re-announced (new eventId) so an order-service that missed it can recover.
    const succeeded = repo.outboxOfType(EVENT_TYPES.PaymentSucceeded);
    expect(succeeded).toHaveLength(2);
    expect(succeeded[0]!.eventId).not.toBe(succeeded[1]!.eventId);
    expect((succeeded[1]!.envelope.payload as { paymentId: string }).paymentId).toBe(request.paymentId);
  });

  it('republishes a FAILED result for a duplicate request without retrying the charge', async () => {
    const { repo, deps } = setup({ random: () => 0.01 });
    const request = makeRequest();

    await processPaymentRequested(deps, makeEnvelope(request));
    const retry = await processPaymentRequested(deps, makeEnvelope(request));

    expect(retry.kind).toBe('duplicate_request');
    expect(repo.payments.get(request.paymentId)!.status).toBe('FAILED');
    expect(repo.payments.get(request.paymentId)!.attemptCount).toBe(1);
    expect(repo.outboxOfType(EVENT_TYPES.PaymentFailed)).toHaveLength(2);
  });

  it('charges only once when two different requests for the same order race', async () => {
    const { repo, deps } = setup();
    const request = makeRequest();

    const [a, b] = await Promise.all([
      processPaymentRequested(deps, makeEnvelope(request)),
      processPaymentRequested(deps, makeEnvelope(request)),
    ]);

    const kinds = [a.kind, b.kind].sort();
    expect(kinds).toEqual(['duplicate_request', 'processed']);
    expect(repo.payments.size).toBe(1);
    expect(repo.payments.get(request.paymentId)!.attemptCount).toBe(1);
    expect(repo.outbox).toHaveLength(1);
  });

  it('charges only once when the very same event is delivered to two workers at once', async () => {
    const { repo, deps } = setup();
    const request = makeRequest();
    const envelope = makeEnvelope(request);

    const [a, b] = await Promise.all([
      processPaymentRequested(deps, envelope),
      processPaymentRequested(deps, envelope),
    ]);

    expect([a.kind, b.kind].sort()).toEqual(['duplicate_event', 'processed']);
    expect(repo.payments.get(request.paymentId)!.attemptCount).toBe(1);
    expect(repo.outbox).toHaveLength(1);
  });

  it('finishes a payment left PENDING by a crashed worker', async () => {
    const { repo, deps } = setup();
    const request = makeRequest();
    await repo.withTransaction((tx) =>
      tx.insertPendingIfAbsent({
        id: request.paymentId,
        orderId: request.orderId,
        holdId: request.holdId,
        userId: request.userId,
        amountCents: request.amountCents,
        currency: request.currency,
      }),
    );
    expect(repo.payments.get(request.paymentId)!.status).toBe('PENDING');

    const result = await processPaymentRequested(deps, makeEnvelope(request));

    expect(result.kind).toBe('processed');
    expect(repo.payments.get(request.paymentId)!.status).toBe('SUCCEEDED');
    expect(repo.outbox).toHaveLength(1);
  });

  it('rejects a paymentId that already belongs to a different order', async () => {
    const { repo, deps } = setup();
    const first = makeRequest();
    await processPaymentRequested(deps, makeEnvelope(first));

    const clash = makeRequest({ paymentId: first.paymentId });
    await expect(processPaymentRequested(deps, makeEnvelope(clash))).rejects.toThrow(/different order/);
    expect(repo.payments.size).toBe(1);
  });
});

describe('forcePaymentResult', () => {
  async function pendingPayment() {
    const { repo } = setup();
    const request = makeRequest();
    await repo.withTransaction((tx) =>
      tx.insertPendingIfAbsent({
        id: request.paymentId,
        orderId: request.orderId,
        holdId: request.holdId,
        userId: request.userId,
        amountCents: request.amountCents,
        currency: request.currency,
      }),
    );
    return { repo, request };
  }

  it('forces a pending payment to succeed', async () => {
    const { repo, request } = await pendingPayment();
    const payment = await forcePaymentResult({
      repo,
      producerName: 'payment-service',
      paymentId: request.paymentId,
      outcome: 'succeed',
    });
    expect(payment.status).toBe('SUCCEEDED');
    expect(repo.outboxOfType(EVENT_TYPES.PaymentSucceeded)).toHaveLength(1);
  });

  it('forces a pending payment to fail with a custom reason', async () => {
    const { repo, request } = await pendingPayment();
    const payment = await forcePaymentResult({
      repo,
      producerName: 'payment-service',
      paymentId: request.paymentId,
      outcome: 'fail',
      reason: 'TEST_DECLINE',
    });
    expect(payment.status).toBe('FAILED');
    expect(payment.failureReason).toBe('TEST_DECLINE');
    expect(repo.outboxOfType(EVENT_TYPES.PaymentFailed)).toHaveLength(1);
  });

  it('refuses to overwrite a settled payment unless forced', async () => {
    const { repo, request } = await pendingPayment();
    const base = { repo, producerName: 'payment-service', paymentId: request.paymentId };
    await forcePaymentResult({ ...base, outcome: 'succeed' });

    await expect(forcePaymentResult({ ...base, outcome: 'fail' })).rejects.toMatchObject({
      code: 'INVALID_STATE_TRANSITION',
    });

    const overridden = await forcePaymentResult({ ...base, outcome: 'fail', force: true });
    expect(overridden.status).toBe('FAILED');
  });

  it('throws NotFound for an unknown payment', async () => {
    const { repo } = setup();
    await expect(
      forcePaymentResult({ repo, producerName: 'payment-service', paymentId: createId(), outcome: 'succeed' }),
    ).rejects.toMatchObject({ code: 'NOT_FOUND' });
  });
});

describe('handlePaymentMessage', () => {
  it('processes a valid PaymentRequested message', async () => {
    const { repo, deps } = setup();
    const request = makeRequest();
    const outcome = await handlePaymentMessage(deps, Buffer.from(JSON.stringify(makeEnvelope(request))));

    expect(outcome.kind).toBe('handled');
    expect(repo.payments.get(request.paymentId)!.status).toBe('SUCCEEDED');
  });

  it('ignores our own result events on the shared topic', async () => {
    const { repo, deps } = setup();
    const envelope = createEnvelope({
      type: EVENT_TYPES.PaymentSucceeded,
      payload: { anything: true },
      producer: 'payment-service',
    });
    const outcome = await handlePaymentMessage(deps, JSON.stringify(envelope));

    expect(outcome).toEqual({ kind: 'ignored', type: EVENT_TYPES.PaymentSucceeded });
    expect(repo.payments.size).toBe(0);
  });

  it('flags garbage and bad payloads as invalid', async () => {
    const { repo, deps } = setup();

    expect((await handlePaymentMessage(deps, null)).kind).toBe('invalid');
    expect((await handlePaymentMessage(deps, 'not json')).kind).toBe('invalid');
    expect((await handlePaymentMessage(deps, JSON.stringify({ hello: 'world' }))).kind).toBe('invalid');

    const badPayload = createEnvelope({
      type: EVENT_TYPES.PaymentRequested,
      payload: { orderId: 'nope' },
      producer: 'order-service',
    });
    expect((await handlePaymentMessage(deps, JSON.stringify(badPayload))).kind).toBe('invalid');
    expect(repo.payments.size).toBe(0);
  });
});
