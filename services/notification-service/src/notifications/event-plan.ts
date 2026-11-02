import {
  EVENT_TYPES,
  InventoryReservedSchema,
  OrderConfirmedSchema,
  OrderCreatedSchema,
  OrderFailedSchema,
  PaymentFailedSchema,
  PaymentSucceededSchema,
  WaitlistOfferedSchema,
  type EventEnvelope,
} from '@seatlock/shared';
import type { ZodTypeAny, z } from 'zod';

/** What the notification service intends to do about one event. */
export interface EventPlan {
  /** Template name, or null when the event is only used to learn hold ownership. */
  template: string | null;
  /** The recipient, if the event carries it. Otherwise it is resolved through holdId. */
  userId: string | null;
  holdId: string | null;
  /** (hold, user) pair to remember for later events that only carry the holdId. */
  owner: { holdId: string; userId: string } | null;
  /** Event payload, stored in the notification so a template can be rendered later. */
  data: Record<string, unknown>;
}

export type PlanResult =
  | { kind: 'ignored' }
  | { kind: 'invalid'; reason: string }
  | { kind: 'plan'; plan: EventPlan };

/** Event types that produce a user-facing notification. */
export const NOTIFYING_EVENT_TYPES: ReadonlySet<string> = new Set([
  EVENT_TYPES.InventoryReserved,
  EVENT_TYPES.OrderConfirmed,
  EVENT_TYPES.OrderFailed,
  EVENT_TYPES.PaymentSucceeded,
  EVENT_TYPES.PaymentFailed,
  EVENT_TYPES.WaitlistOffered,
]);

export const TEMPLATES = {
  InventoryReserved: 'tickets_reserved',
  OrderConfirmed: 'order_confirmed',
  OrderFailed: 'order_failed',
  PaymentSucceeded: 'payment_succeeded',
  PaymentFailed: 'payment_failed',
  WaitlistOffered: 'waitlist_offer',
} as const;

function parse<S extends ZodTypeAny>(
  schema: S,
  payload: unknown,
  type: string,
  build: (data: z.infer<S>) => EventPlan,
): PlanResult {
  const result = schema.safeParse(payload);
  if (!result.success) {
    const issues = result.error.issues.map((i) => `${i.path.join('.')} ${i.message}`).join('; ');
    return { kind: 'invalid', reason: `Invalid ${type} payload: ${issues}` };
  }
  return { kind: 'plan', plan: build(result.data as z.infer<S>) };
}

/** Validate an event's payload and decide what to do with it. Pure; no I/O. */
export function planEvent(envelope: EventEnvelope): PlanResult {
  const { type, payload } = envelope;

  switch (type) {
    case EVENT_TYPES.InventoryReserved:
      return parse(InventoryReservedSchema, payload, type, (p) => ({
        template: TEMPLATES.InventoryReserved,
        userId: p.userId,
        holdId: p.holdId,
        owner: { holdId: p.holdId, userId: p.userId },
        data: p,
      }));

    case EVENT_TYPES.OrderCreated:
      // No notification, but it tells us who owns the hold.
      return parse(OrderCreatedSchema, payload, type, (p) => ({
        template: null,
        userId: p.userId,
        holdId: p.holdId,
        owner: { holdId: p.holdId, userId: p.userId },
        data: p,
      }));

    case EVENT_TYPES.OrderConfirmed:
      return parse(OrderConfirmedSchema, payload, type, (p) => ({
        template: TEMPLATES.OrderConfirmed,
        userId: p.userId,
        holdId: p.holdId,
        owner: { holdId: p.holdId, userId: p.userId },
        data: p,
      }));

    case EVENT_TYPES.OrderFailed:
      return parse(OrderFailedSchema, payload, type, (p) => ({
        template: TEMPLATES.OrderFailed,
        userId: null,
        holdId: p.holdId,
        owner: null,
        data: p,
      }));

    case EVENT_TYPES.PaymentSucceeded:
      return parse(PaymentSucceededSchema, payload, type, (p) => ({
        template: TEMPLATES.PaymentSucceeded,
        userId: null,
        holdId: p.holdId,
        owner: null,
        data: p,
      }));

    case EVENT_TYPES.PaymentFailed:
      return parse(PaymentFailedSchema, payload, type, (p) => ({
        template: TEMPLATES.PaymentFailed,
        userId: null,
        holdId: p.holdId,
        owner: null,
        data: p,
      }));

    case EVENT_TYPES.WaitlistOffered:
      return parse(WaitlistOfferedSchema, payload, type, (p) => ({
        template: TEMPLATES.WaitlistOffered,
        userId: p.userId,
        holdId: p.holdId,
        owner: { holdId: p.holdId, userId: p.userId },
        data: p,
      }));

    default:
      return { kind: 'ignored' };
  }
}
