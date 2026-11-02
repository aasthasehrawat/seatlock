import { z } from 'zod';
import { createId, nowIso } from '../utils.js';
import { EVENT_TYPE_VALUES, type EventType } from './types.js';

/**
 * Every message on Kafka is wrapped in this envelope.
 * - eventId:       unique per message; consumers use it for idempotent processing
 * - correlationId: shared by all events in one user flow; used for tracing
 * - version:       payload schema version, so schemas can evolve safely
 */
export const EventEnvelopeSchema = z.object({
  eventId: z.string().uuid(),
  type: z.enum(EVENT_TYPE_VALUES),
  version: z.number().int().positive(),
  occurredAt: z.string().datetime(),
  correlationId: z.string().min(1),
  producer: z.string().min(1),
  payload: z.unknown(),
});

/** Envelope with an untyped payload. Validate the payload separately using its schema. */
export type EventEnvelope<TPayload = unknown> = Omit<
  z.infer<typeof EventEnvelopeSchema>,
  'payload'
> & { payload: TPayload };

export interface CreateEnvelopeInput<TPayload> {
  type: EventType;
  payload: TPayload;
  producer: string;
  correlationId?: string;
  version?: number;
}

/** Build a new envelope. A fresh correlationId is generated if the caller has none. */
export function createEnvelope<TPayload>(input: CreateEnvelopeInput<TPayload>): EventEnvelope<TPayload> {
  return {
    eventId: createId(),
    type: input.type,
    version: input.version ?? 1,
    occurredAt: nowIso(),
    correlationId: input.correlationId ?? createId(),
    producer: input.producer,
    payload: input.payload,
  };
}
