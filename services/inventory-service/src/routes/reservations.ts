import type { FastifyPluginAsync } from 'fastify';
import { z } from 'zod';
import type { AppContext } from '../context.js';
import { checkRateLimit } from '../lib/redis.js';
import { reserveTickets } from '../services/reservation.js';
import { parseOrThrow, requireIdempotencyKey } from './helpers.js';

export const reservationRoutes: FastifyPluginAsync<{ ctx: AppContext }> = async (app, { ctx }) => {
  const BodySchema = z.object({
    eventId: z.string().uuid(),
    userId: z.string().uuid(),
    quantity: z.number().int().min(1).max(ctx.config.maxTicketsPerReservation),
  });

  /**
   * POST /reservations: the hot path of the flash sale.
   *
   * Order of checks is cheapest-first:
   *   1. per-IP rate limit (Redis, one round trip)  -> 429
   *   2. header + body validation (no I/O)           -> 400
   *   3. the atomic reservation transaction          -> 201 | 404 | 409
   */
  app.post('/reservations', async (request, reply) => {
    const { max, windowSeconds } = ctx.config.rateLimit;
    const limit = await checkRateLimit(ctx.redis, `ratelimit:reservations:${request.ip}`, max, windowSeconds);
    reply.header('X-RateLimit-Limit', limit.limit).header('X-RateLimit-Remaining', limit.remaining);
    if (!limit.allowed) {
      return reply
        .code(429)
        .header('Retry-After', limit.retryAfterSeconds)
        .send({
          error: {
            code: 'RATE_LIMITED',
            message: `Too many reservation attempts; retry in ${limit.retryAfterSeconds}s`,
          },
        });
    }

    const idempotencyKey = requireIdempotencyKey(request);
    const body = parseOrThrow(BodySchema, request.body, 'reservation request');

    const result = await reserveTickets(ctx.services, {
      ...body,
      idempotencyKey,
      correlationId: request.correlationId,
    });

    return reply
      .code(result.status)
      .header('Idempotent-Replayed', String(result.replayed))
      .send(result.body);
  });
};
