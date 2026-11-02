import { z } from 'zod';
import { EVENT_SCHEMAS, EVENT_TYPES, type EventPayloadMap, type EventType } from '@seatlock/shared';
import { PoisonMessageError } from '../lib/errors.js';
import type { EventMeta, EventResult, OrderService } from '../services/order-service.js';

/** Just enough envelope validation to route; payloads are validated per event type. */
const RoutingEnvelopeSchema = z.object({
  eventId: z.string().uuid(),
  type: z.string().min(1),
  correlationId: z.string().min(1),
  payload: z.unknown(),
});

export interface ParsedMessage {
  meta: EventMeta;
  type: string;
  payload: unknown;
}

/**
 * Decode a Kafka message value. Anything undecodable throws PoisonMessageError, which the
 * consumer sends straight to the DLQ: retrying garbage forever helps nobody.
 */
export function parseMessage(value: Buffer | null): ParsedMessage {
  if (!value) throw new PoisonMessageError('Message has an empty value');

  let json: unknown;
  try {
    json = JSON.parse(value.toString('utf8'));
  } catch {
    throw new PoisonMessageError('Message value is not valid JSON');
  }

  const result = RoutingEnvelopeSchema.safeParse(json);
  if (!result.success) {
    throw new PoisonMessageError(`Invalid event envelope: ${result.error.message}`);
  }

  const { eventId, correlationId, type, payload } = result.data;
  return { meta: { eventId, correlationId }, type, payload };
}

/** Process one parsed message. Resolves on success/skip; throws on failure (retry or DLQ). */
export type MessageDispatcher = (message: ParsedMessage) => Promise<EventResult>;

type Handler = (meta: EventMeta, payload: unknown) => Promise<EventResult>;

/** Bind an event type to its Zod schema and a typed service method. */
function handle<T extends EventType>(
  type: T,
  fn: (meta: EventMeta, payload: EventPayloadMap[T]) => Promise<EventResult>,
): Handler {
  return async (meta, rawPayload) => {
    const parsed = EVENT_SCHEMAS[type].safeParse(rawPayload);
    if (!parsed.success) {
      throw new PoisonMessageError(`Invalid ${type} payload: ${parsed.error.message}`);
    }
    return fn(meta, parsed.data as EventPayloadMap[T]);
  };
}

/**
 * Routes events to the service by `type`. Unknown or irrelevant types are ignored (not DLQ'd):
 * other services share these topics and may legitimately publish events we do not care about.
 */
export function createDispatcher(service: OrderService): MessageDispatcher {
  const handlers = new Map<string, Handler>([
    [EVENT_TYPES.InventoryReserved, handle(EVENT_TYPES.InventoryReserved, (m, p) => service.onInventoryReserved(m, p))],
    [EVENT_TYPES.InventoryReleased, handle(EVENT_TYPES.InventoryReleased, (m, p) => service.onInventoryReleased(m, p))],
    [EVENT_TYPES.HoldExpired, handle(EVENT_TYPES.HoldExpired, (m, p) => service.onHoldExpired(m, p))],
    [EVENT_TYPES.PaymentSucceeded, handle(EVENT_TYPES.PaymentSucceeded, (m, p) => service.onPaymentSucceeded(m, p))],
    [EVENT_TYPES.PaymentFailed, handle(EVENT_TYPES.PaymentFailed, (m, p) => service.onPaymentFailed(m, p))],
  ]);

  return async (message) => {
    const handler = handlers.get(message.type);
    if (!handler) return { outcome: 'ignored', reason: `unhandled_event_type_${message.type}` };
    return handler(message.meta, message.payload);
  };
}
