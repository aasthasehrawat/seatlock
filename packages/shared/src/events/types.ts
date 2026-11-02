/**
 * Every business event in the platform has exactly one type constant.
 * The string value equals the key, so logs and Kafka headers stay readable.
 */
export const EVENT_TYPES = {
  InventoryReserved: 'InventoryReserved',
  InventoryReleased: 'InventoryReleased',
  InventoryConfirmed: 'InventoryConfirmed',
  PaymentRequested: 'PaymentRequested',
  PaymentSucceeded: 'PaymentSucceeded',
  PaymentFailed: 'PaymentFailed',
  OrderCreated: 'OrderCreated',
  OrderConfirmed: 'OrderConfirmed',
  OrderFailed: 'OrderFailed',
  OrderExpired: 'OrderExpired',
  HoldExpired: 'HoldExpired',
  WaitlistOffered: 'WaitlistOffered',
  NotificationRequested: 'NotificationRequested',
} as const;

export type EventType = (typeof EVENT_TYPES)[keyof typeof EVENT_TYPES];

/** Tuple of all event types; handy for z.enum() and iteration. */
export const EVENT_TYPE_VALUES = Object.values(EVENT_TYPES) as [EventType, ...EventType[]];
