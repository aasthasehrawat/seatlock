import { z } from 'zod';
import { EVENT_TYPES, type EventType } from './types.js';

// Reusable building blocks
const id = z.string().uuid();
const quantity = z.number().int().positive();
const cents = z.number().int().nonnegative();
const timestamp = z.string().datetime();

/** Why inventory was given back to the pool. */
export const ReleaseReasonSchema = z.enum(['EXPIRED', 'PAYMENT_FAILED', 'RELEASED', 'CANCELLED']);
export type ReleaseReason = z.infer<typeof ReleaseReasonSchema>;

export const NotificationChannelSchema = z.enum(['EMAIL', 'SMS', 'PUSH']);
export type NotificationChannel = z.infer<typeof NotificationChannelSchema>;

// Note: `eventId` in payloads is the concert/event being sold,
// not the envelope's eventId (the message id).

export const InventoryReservedSchema = z.object({
  holdId: id,
  eventId: id,
  userId: id,
  quantity,
  unitPriceCents: cents,
  expiresAt: timestamp,
  reservationId: id,
});

export const InventoryReleasedSchema = z.object({
  holdId: id,
  eventId: id,
  quantity,
  reason: ReleaseReasonSchema,
});

export const InventoryConfirmedSchema = z.object({
  holdId: id,
  eventId: id,
  orderId: id,
  quantity,
});

export const PaymentRequestedSchema = z.object({
  paymentId: id,
  orderId: id,
  holdId: id,
  userId: id,
  amountCents: cents,
  currency: z.string().length(3),
});

export const PaymentSucceededSchema = z.object({
  paymentId: id,
  orderId: id,
  holdId: id,
  amountCents: cents,
  providerRef: z.string().min(1),
});

export const PaymentFailedSchema = z.object({
  paymentId: id,
  orderId: id,
  holdId: id,
  amountCents: cents,
  reason: z.string().min(1),
});

export const OrderCreatedSchema = z.object({
  orderId: id,
  holdId: id,
  eventId: id,
  userId: id,
  quantity,
  amountCents: cents,
});

export const OrderConfirmedSchema = z.object({
  orderId: id,
  holdId: id,
  eventId: id,
  userId: id,
  quantity,
  amountCents: cents,
  paymentId: id,
});

export const OrderFailedSchema = z.object({
  orderId: id,
  holdId: id,
  eventId: id,
  reason: z.string().min(1),
});

export const OrderExpiredSchema = z.object({
  orderId: id,
  holdId: id,
  eventId: id,
});

export const HoldExpiredSchema = z.object({
  holdId: id,
  eventId: id,
  quantity,
});

export const WaitlistOfferedSchema = z.object({
  waitlistId: id,
  eventId: id,
  userId: id,
  quantity,
  holdId: id,
  expiresAt: timestamp,
});

export const NotificationRequestedSchema = z.object({
  notificationId: id,
  userId: id,
  channel: NotificationChannelSchema,
  template: z.string().min(1),
  data: z.record(z.unknown()),
});

/** Maps each event type to the Zod schema of its payload. */
export const EVENT_SCHEMAS = {
  [EVENT_TYPES.InventoryReserved]: InventoryReservedSchema,
  [EVENT_TYPES.InventoryReleased]: InventoryReleasedSchema,
  [EVENT_TYPES.InventoryConfirmed]: InventoryConfirmedSchema,
  [EVENT_TYPES.PaymentRequested]: PaymentRequestedSchema,
  [EVENT_TYPES.PaymentSucceeded]: PaymentSucceededSchema,
  [EVENT_TYPES.PaymentFailed]: PaymentFailedSchema,
  [EVENT_TYPES.OrderCreated]: OrderCreatedSchema,
  [EVENT_TYPES.OrderConfirmed]: OrderConfirmedSchema,
  [EVENT_TYPES.OrderFailed]: OrderFailedSchema,
  [EVENT_TYPES.OrderExpired]: OrderExpiredSchema,
  [EVENT_TYPES.HoldExpired]: HoldExpiredSchema,
  [EVENT_TYPES.WaitlistOffered]: WaitlistOfferedSchema,
  [EVENT_TYPES.NotificationRequested]: NotificationRequestedSchema,
} as const satisfies Record<EventType, z.ZodTypeAny>;

/** Maps each event type to its inferred payload TypeScript type. */
export type EventPayloadMap = {
  [K in EventType]: z.infer<(typeof EVENT_SCHEMAS)[K]>;
};

export type InventoryReservedPayload = EventPayloadMap['InventoryReserved'];
export type InventoryReleasedPayload = EventPayloadMap['InventoryReleased'];
export type InventoryConfirmedPayload = EventPayloadMap['InventoryConfirmed'];
export type PaymentRequestedPayload = EventPayloadMap['PaymentRequested'];
export type PaymentSucceededPayload = EventPayloadMap['PaymentSucceeded'];
export type PaymentFailedPayload = EventPayloadMap['PaymentFailed'];
export type OrderCreatedPayload = EventPayloadMap['OrderCreated'];
export type OrderConfirmedPayload = EventPayloadMap['OrderConfirmed'];
export type OrderFailedPayload = EventPayloadMap['OrderFailed'];
export type OrderExpiredPayload = EventPayloadMap['OrderExpired'];
export type HoldExpiredPayload = EventPayloadMap['HoldExpired'];
export type WaitlistOfferedPayload = EventPayloadMap['WaitlistOffered'];
export type NotificationRequestedPayload = EventPayloadMap['NotificationRequested'];
