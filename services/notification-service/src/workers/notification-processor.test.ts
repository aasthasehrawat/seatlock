import { describe, expect, it, vi } from 'vitest';
import {
  EVENT_TYPES,
  createEnvelope,
  createId,
  type EventEnvelope,
  type EventType,
} from '@seatlock/shared';
import { createLogger } from '../lib/logger.js';
import { InMemoryNotificationRepository } from '../test-support/in-memory-repository.js';
import type { NotificationSender } from '../notifications/types.js';
import { handleEvent, handleMessage, type ProcessorDeps } from './notification-processor.js';

const logger = createLogger({ level: 'silent', name: 'test' });

const userId = createId();
const holdId = createId();
const eventId = createId(); // the concert, not the message
const orderId = createId();
const paymentId = createId();

function envelope(type: EventType, payload: unknown): EventEnvelope {
  return createEnvelope({ type, payload, producer: 'test', correlationId: 'corr-1' });
}

const reserved = () =>
  envelope(EVENT_TYPES.InventoryReserved, {
    holdId,
    eventId,
    userId,
    quantity: 2,
    unitPriceCents: 5000,
    expiresAt: new Date().toISOString(),
    reservationId: createId(),
  });

const orderConfirmed = () =>
  envelope(EVENT_TYPES.OrderConfirmed, {
    orderId,
    holdId,
    eventId,
    userId,
    quantity: 2,
    amountCents: 10_000,
    paymentId,
  });

const orderFailed = () => envelope(EVENT_TYPES.OrderFailed, { orderId, holdId, eventId, reason: 'PAYMENT_FAILED' });

const paymentSucceeded = () =>
  envelope(EVENT_TYPES.PaymentSucceeded, { paymentId, orderId, holdId, amountCents: 10_000, providerRef: 'mock_1' });

const paymentFailed = () =>
  envelope(EVENT_TYPES.PaymentFailed, { paymentId, orderId, holdId, amountCents: 10_000, reason: 'CARD_DECLINED' });

const waitlistOffered = () =>
  envelope(EVENT_TYPES.WaitlistOffered, {
    waitlistId: createId(),
    eventId,
    userId,
    quantity: 1,
    holdId: createId(),
    expiresAt: new Date().toISOString(),
  });

function setup(overrides: Partial<ProcessorDeps> = {}) {
  const repo = new InMemoryNotificationRepository();
  const send = vi.fn(async () => undefined);
  const sender: NotificationSender = { send };
  const sleep = vi.fn(async () => undefined);
  const deps: ProcessorDeps = {
    repo,
    sender,
    logger,
    userLookupRetries: 2,
    userLookupDelayMs: 10,
    sleep,
    ...overrides,
  };
  return { repo, deps, send, sleep };
}

describe('handleEvent', () => {
  it.each([
    ['InventoryReserved', reserved, 'tickets_reserved'],
    ['OrderConfirmed', orderConfirmed, 'order_confirmed'],
    ['WaitlistOffered', waitlistOffered, 'waitlist_offer'],
  ] as const)('notifies the user directly named in %s', async (_name, make, template) => {
    const { repo, deps, send } = setup();
    const event = make();

    const result = await handleEvent(deps, event);

    expect(result.kind).toBe('notified');
    expect(repo.notifications).toHaveLength(1);
    const n = repo.notifications[0]!;
    expect(n.userId).toBe(userId);
    expect(n.channel).toBe('EMAIL');
    expect(n.template).toBe(template);
    expect(n.status).toBe('SENT');
    expect(n.payload).toMatchObject({ event: event.type, sourceEventId: event.eventId, correlationId: 'corr-1' });
    expect(send).toHaveBeenCalledTimes(1);
    expect(repo.processedEvents.has(event.eventId)).toBe(true);
  });

  it.each([
    ['OrderFailed', orderFailed, 'order_failed'],
    ['PaymentSucceeded', paymentSucceeded, 'payment_succeeded'],
    ['PaymentFailed', paymentFailed, 'payment_failed'],
  ] as const)('resolves the user for %s through the hold owner', async (_name, make, template) => {
    const { repo, deps } = setup();
    repo.holdOwners.set(holdId, userId);

    const result = await handleEvent(deps, make());

    expect(result.kind).toBe('notified');
    expect(repo.notifications).toHaveLength(1);
    expect(repo.notifications[0]).toMatchObject({ userId, template, status: 'SENT' });
  });

  it('learns hold ownership from InventoryReserved so later payment events resolve', async () => {
    const { repo, deps } = setup();

    await handleEvent(deps, reserved());
    expect(repo.holdOwners.get(holdId)).toBe(userId);

    const result = await handleEvent(deps, paymentFailed());
    expect(result.kind).toBe('notified');
    expect(repo.notifications.map((n) => n.template)).toEqual(['tickets_reserved', 'payment_failed']);
  });

  it('records the hold owner from OrderCreated without notifying', async () => {
    const { repo, deps, send } = setup();
    const created = envelope(EVENT_TYPES.OrderCreated, {
      orderId,
      holdId,
      eventId,
      userId,
      quantity: 2,
      amountCents: 10_000,
    });

    const result = await handleEvent(deps, created);

    expect(result.kind).toBe('recorded');
    expect(repo.holdOwners.get(holdId)).toBe(userId);
    expect(repo.notifications).toHaveLength(0);
    expect(send).not.toHaveBeenCalled();
    expect(repo.processedEvents.has(created.eventId)).toBe(true);
  });

  it('is idempotent: the same event delivered twice creates one notification', async () => {
    const { repo, deps, send } = setup();
    const event = reserved();

    const first = await handleEvent(deps, event);
    const second = await handleEvent(deps, event);

    expect(first.kind).toBe('notified');
    expect(second.kind).toBe('duplicate');
    expect(repo.notifications).toHaveLength(1);
    expect(send).toHaveBeenCalledTimes(1);
  });

  it('is idempotent when the same event is processed concurrently', async () => {
    const { repo, deps, send } = setup();
    const event = orderConfirmed();

    const results = await Promise.all([handleEvent(deps, event), handleEvent(deps, event)]);

    expect(results.map((r) => r.kind).sort()).toEqual(['duplicate', 'notified']);
    expect(repo.notifications).toHaveLength(1);
    expect(send).toHaveBeenCalledTimes(1);
  });

  it('different events for the same user each produce a notification', async () => {
    const { repo, deps } = setup();
    await handleEvent(deps, orderConfirmed());
    await handleEvent(deps, orderConfirmed());
    expect(repo.notifications).toHaveLength(2);
  });

  it('retries the owner lookup and then reports the event as unresolved', async () => {
    const { repo, deps, sleep } = setup({ userLookupRetries: 2, userLookupDelayMs: 10 });

    const result = await handleEvent(deps, paymentSucceeded());

    expect(result).toEqual({ kind: 'unresolved', holdId });
    expect(sleep).toHaveBeenCalledTimes(2);
    expect(repo.notifications).toHaveLength(0);
    expect(repo.processedEvents.size).toBe(0); // not marked, so a DLQ replay can succeed later
  });

  it('succeeds if the owner shows up during the lookup retries', async () => {
    const { repo, deps, sleep } = setup();
    sleep.mockImplementationOnce(async () => {
      repo.holdOwners.set(holdId, userId);
    });

    const result = await handleEvent(deps, paymentSucceeded());

    expect(result.kind).toBe('notified');
    expect(sleep).toHaveBeenCalledTimes(1);
  });

  it('marks the notification FAILED when delivery throws, but still records it', async () => {
    const { repo, deps } = setup({
      sender: {
        send: async () => {
          throw new Error('smtp down');
        },
      },
    });

    const result = await handleEvent(deps, reserved());

    expect(result.kind).toBe('notified');
    expect(repo.notifications[0]!.status).toBe('FAILED');
    expect(repo.processedEvents.size).toBe(1);
  });

  it('ignores events it does not notify on', async () => {
    const { repo, deps, send } = setup();
    const released = envelope(EVENT_TYPES.InventoryReleased, {
      holdId,
      eventId,
      quantity: 1,
      reason: 'EXPIRED',
    });

    expect(await handleEvent(deps, released)).toEqual({ kind: 'ignored', type: 'InventoryReleased' });
    expect(repo.notifications).toHaveLength(0);
    expect(repo.processedEvents.size).toBe(0);
    expect(send).not.toHaveBeenCalled();
  });

  it('reports an invalid payload', async () => {
    const { repo, deps } = setup();
    const result = await handleEvent(deps, envelope(EVENT_TYPES.OrderConfirmed, { orderId: 'nope' }));
    expect(result.kind).toBe('invalid');
    expect(repo.notifications).toHaveLength(0);
  });
});

describe('handleMessage', () => {
  it('decodes a raw message and creates a notification', async () => {
    const { repo, deps } = setup();
    const result = await handleMessage(deps, Buffer.from(JSON.stringify(orderConfirmed())));
    expect(result.kind).toBe('notified');
    expect(repo.notifications).toHaveLength(1);
  });

  it('flags undecodable messages as invalid', async () => {
    const { deps } = setup();
    expect((await handleMessage(deps, null)).kind).toBe('invalid');
    expect((await handleMessage(deps, 'garbage')).kind).toBe('invalid');
    expect((await handleMessage(deps, JSON.stringify({ hello: 'world' }))).kind).toBe('invalid');
  });
});
