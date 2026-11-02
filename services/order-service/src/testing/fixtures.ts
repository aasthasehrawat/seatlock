import pino from 'pino';
import {
  createId,
  type HoldExpiredPayload,
  type InventoryReleasedPayload,
  type InventoryReservedPayload,
  type PaymentFailedPayload,
  type PaymentSucceededPayload,
} from '@seatlock/shared';
import type { Order } from '../db/types.js';
import { OrderService, type EventMeta } from '../services/order-service.js';
import { InMemoryUnitOfWork } from './in-memory.js';

export const silentLogger = pino({ level: 'silent' });

export interface Harness {
  uow: InMemoryUnitOfWork;
  service: OrderService;
}

export function createHarness(): Harness {
  const uow = new InMemoryUnitOfWork();
  const service = new OrderService({ uow, logger: silentLogger, defaultCurrency: 'USD' });
  return { uow, service };
}

/** Fresh message identity: every call is a "new Kafka message". */
export function meta(overrides: Partial<EventMeta> = {}): EventMeta {
  return { eventId: createId(), correlationId: 'corr-test', ...overrides };
}

export function inventoryReserved(
  overrides: Partial<InventoryReservedPayload> = {},
): InventoryReservedPayload {
  return {
    holdId: createId(),
    eventId: createId(),
    userId: createId(),
    quantity: 2,
    unitPriceCents: 5000,
    expiresAt: new Date(Date.now() + 300_000).toISOString(),
    reservationId: createId(),
    ...overrides,
  };
}

/** Create an order through the real consumer path and return it. */
export async function seedPendingOrder(
  h: Harness,
  overrides: Partial<InventoryReservedPayload> = {},
): Promise<Order> {
  const result = await h.service.onInventoryReserved(meta(), inventoryReserved(overrides));
  const order = result.orderId ? h.uow.getOrder(result.orderId) : undefined;
  if (!order) throw new Error('seedPendingOrder: order was not created');
  return order;
}

/** Create an order and start its payment. */
export async function seedOrderWithPayment(h: Harness): Promise<Order> {
  const order = await seedPendingOrder(h);
  await h.service.initiatePayment({ orderId: order.id, idempotencyKey: `key-${createId()}` });
  return h.uow.getOrder(order.id) as Order;
}

export function paymentSucceeded(
  order: Order,
  overrides: Partial<PaymentSucceededPayload> = {},
): PaymentSucceededPayload {
  return {
    paymentId: order.paymentId ?? createId(),
    orderId: order.id,
    holdId: order.holdId,
    amountCents: order.amountCents,
    providerRef: 'prov_123',
    ...overrides,
  };
}

export function paymentFailed(
  order: Order,
  overrides: Partial<PaymentFailedPayload> = {},
): PaymentFailedPayload {
  return {
    paymentId: order.paymentId ?? createId(),
    orderId: order.id,
    holdId: order.holdId,
    amountCents: order.amountCents,
    reason: 'card_declined',
    ...overrides,
  };
}

export function inventoryReleased(
  order: Order,
  reason: InventoryReleasedPayload['reason'],
): InventoryReleasedPayload {
  return { holdId: order.holdId, eventId: order.eventId, quantity: order.quantity, reason };
}

export function holdExpired(order: Order): HoldExpiredPayload {
  return { holdId: order.holdId, eventId: order.eventId, quantity: order.quantity };
}
