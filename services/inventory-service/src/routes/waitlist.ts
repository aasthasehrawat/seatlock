import type { FastifyPluginAsync } from 'fastify';
import { z } from 'zod';
import type { AppContext } from '../context.js';
import { joinWaitlist } from '../services/waitlist.js';
import { parseOrThrow, requireIdempotencyKey } from './helpers.js';

export const waitlistRoutes: FastifyPluginAsync<{ ctx: AppContext }> = async (app, { ctx }) => {
  const BodySchema = z.object({
    eventId: z.string().uuid(),
    userId: z.string().uuid(),
    quantity: z.number().int().min(1).max(ctx.config.maxTicketsPerReservation),
  });

  app.post('/waitlist', async (request, reply) => {
    const idempotencyKey = requireIdempotencyKey(request);
    const body = parseOrThrow(BodySchema, request.body, 'waitlist request');

    const result = await joinWaitlist(ctx.services, {
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
