import type { FastifyRequest } from 'fastify';
import { ValidationError } from '@seatlock/shared';
import { z, type ZodTypeAny } from 'zod';

/** Parse with zod and convert failures into the shared 400 ValidationError. */
export function parseOrThrow<S extends ZodTypeAny>(schema: S, data: unknown, what: string): z.infer<S> {
  const result = schema.safeParse(data);
  if (!result.success) {
    throw new ValidationError(`Invalid ${what}`, result.error.flatten());
  }
  return result.data;
}

const IdempotencyKeySchema = z.string().trim().min(1).max(255);

/** Mutating endpoints require a client-generated Idempotency-Key so retries are safe. */
export function requireIdempotencyKey(request: FastifyRequest): string {
  const raw = request.headers['idempotency-key'];
  const parsed = IdempotencyKeySchema.safeParse(Array.isArray(raw) ? raw[0] : raw);
  if (!parsed.success) {
    throw new ValidationError('Idempotency-Key header is required (1-255 characters)');
  }
  return parsed.data;
}

export const uuidParam = z.object({ id: z.string().uuid() });
