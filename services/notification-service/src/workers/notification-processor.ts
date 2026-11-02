import { v4 as uuidv4 } from 'uuid';
import type { EventEnvelope, NotificationChannel } from '@seatlock/shared';
import { decodeEnvelope } from '../lib/kafka.js';
import type { Logger } from '../lib/logger.js';
import { planEvent } from '../notifications/event-plan.js';
import type { NewNotification, NotificationRepository, NotificationSender } from '../notifications/types.js';

export interface ProcessorDeps {
  repo: NotificationRepository;
  sender: NotificationSender;
  logger: Logger;
  /** Channel used for every notification (the mock only does email). */
  channel?: NotificationChannel;
  /** Retry the hold-owner lookup this many times (events from different topics can race). */
  userLookupRetries: number;
  userLookupDelayMs: number;
  /** Injectable so tests don't actually wait. */
  sleep?: (ms: number) => Promise<void>;
}

export type HandleResult =
  /** Not an event this service cares about. */
  | { kind: 'ignored'; type: string }
  /** Payload failed schema validation. Should go to the DLQ. */
  | { kind: 'invalid'; reason: string }
  /** This event was already handled; nothing was written. */
  | { kind: 'duplicate' }
  /** We couldn't work out who to notify. Should go to the DLQ. */
  | { kind: 'unresolved'; holdId: string }
  /** Event was recorded (hold owner learned) but needs no notification. */
  | { kind: 'recorded' }
  /** A notification was created. */
  | { kind: 'notified'; notification: NewNotification };

const defaultSleep = (ms: number): Promise<void> => new Promise((resolve) => setTimeout(resolve, ms));

async function resolveUserId(deps: ProcessorDeps, holdId: string): Promise<string | null> {
  const sleep = deps.sleep ?? defaultSleep;
  for (let attempt = 0; attempt <= deps.userLookupRetries; attempt += 1) {
    const userId = await deps.repo.findUserIdByHold(holdId);
    if (userId) return userId;
    if (attempt < deps.userLookupRetries && deps.userLookupDelayMs > 0) {
      await sleep(deps.userLookupDelayMs);
    }
  }
  return null;
}

/**
 * Handle one event from inventory.events / orders.events / payments.events.
 *
 * Idempotency: inserting into processed_events, the optional hold-owner upsert, and the
 * notification insert all happen in ONE transaction. A redelivered event hits the
 * processed_events conflict and does nothing; a crash before commit leaves nothing behind.
 */
export async function handleEvent(deps: ProcessorDeps, envelope: EventEnvelope): Promise<HandleResult> {
  const log = deps.logger.child({ eventId: envelope.eventId, type: envelope.type, correlationId: envelope.correlationId });

  const planned = planEvent(envelope);
  if (planned.kind === 'ignored') {
    return { kind: 'ignored', type: envelope.type };
  }
  if (planned.kind === 'invalid') {
    return { kind: 'invalid', reason: planned.reason };
  }
  const { plan } = planned;

  // Cheap pre-check; the authoritative check is markEventProcessed inside the transaction.
  if (await deps.repo.isEventProcessed(envelope.eventId)) {
    log.debug('Event already processed; skipping');
    return { kind: 'duplicate' };
  }

  let userId = plan.userId;
  if (plan.template && !userId) {
    if (!plan.holdId) {
      return { kind: 'invalid', reason: `${envelope.type} carries neither userId nor holdId` };
    }
    userId = await resolveUserId(deps, plan.holdId);
    if (!userId) {
      log.warn({ holdId: plan.holdId }, 'Could not resolve user for hold');
      return { kind: 'unresolved', holdId: plan.holdId };
    }
  }

  return deps.repo.withTransaction(async (tx): Promise<HandleResult> => {
    const firstTime = await tx.markEventProcessed(envelope.eventId, envelope.type);
    if (!firstTime) {
      return { kind: 'duplicate' };
    }

    if (plan.owner) {
      await tx.upsertHoldOwner(plan.owner.holdId, plan.owner.userId);
    }

    if (!plan.template || !userId) {
      return { kind: 'recorded' };
    }

    const notification: NewNotification = {
      id: uuidv4(),
      userId,
      channel: deps.channel ?? 'EMAIL',
      template: plan.template,
      payload: {
        event: envelope.type,
        sourceEventId: envelope.eventId,
        correlationId: envelope.correlationId,
        data: plan.data,
      },
      status: 'SENT',
    };

    try {
      await deps.sender.send(notification);
    } catch (err) {
      notification.status = 'FAILED';
      log.error({ err, notificationId: notification.id }, 'Notification delivery failed');
    }

    await tx.insertNotification(notification);
    log.info(
      { notificationId: notification.id, userId, template: notification.template, status: notification.status },
      'Notification recorded',
    );
    return { kind: 'notified', notification };
  });
}

/** Decode and dispatch one raw Kafka message. */
export async function handleMessage(
  deps: ProcessorDeps,
  value: Buffer | string | null | undefined,
): Promise<HandleResult> {
  const decoded = decodeEnvelope(value);
  if (!decoded.ok) {
    return { kind: 'invalid', reason: decoded.reason };
  }
  return handleEvent(deps, decoded.envelope);
}
