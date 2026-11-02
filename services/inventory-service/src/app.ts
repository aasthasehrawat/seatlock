import cors from '@fastify/cors';
import Fastify, { type FastifyError } from 'fastify';
import { AppError, ValidationError } from '@seatlock/shared';
import { ZodError } from 'zod';
import type { AppContext } from './context.js';
import { CORRELATION_HEADER, resolveCorrelationId, runWithCorrelationId } from './lib/correlation.js';
import { adminRoutes } from './routes/admin.js';
import { eventRoutes } from './routes/events.js';
import { healthRoutes } from './routes/health.js';
import { holdRoutes } from './routes/holds.js';
import { reservationRoutes } from './routes/reservations.js';
import { waitlistRoutes } from './routes/waitlist.js';

declare module 'fastify' {
  interface FastifyRequest {
    correlationId: string;
  }
}

/** Consistent error body for every failure: { error: { code, message, details? } }. */
function errorBody(code: string, message: string, details?: unknown) {
  return { error: { code, message, ...(details === undefined ? {} : { details }) } };
}

export async function buildApp(ctx: AppContext) {
  const app = Fastify({
    loggerInstance: ctx.logger,
    trustProxy: ctx.config.trustProxy,
    // Small bodies only: our payloads are tiny JSON objects.
    bodyLimit: 16 * 1024,
    // Reuse the caller's correlation id as the request id so every log line lines up.
    genReqId: (req) => resolveCorrelationId(req.headers[CORRELATION_HEADER]),
  });

  await app.register(cors, { origin: true });

  // --- Correlation id: accept or mint, echo back, expose to logger via AsyncLocalStorage. ---
  app.decorateRequest('correlationId', '');
  app.addHook('onRequest', (request, reply, done) => {
    request.correlationId = request.id;
    reply.header(CORRELATION_HEADER, request.id);
    runWithCorrelationId(request.id, done);
  });

  // --- Errors: map domain errors to HTTP, never leak internals on 500. ---
  app.setErrorHandler((error: FastifyError | Error, request, reply) => {
    if (error instanceof ZodError) {
      const v = new ValidationError('Validation failed', error.flatten());
      return reply.code(v.statusCode).send(errorBody(v.code, v.message, v.details));
    }
    if (error instanceof ValidationError) {
      return reply.code(error.statusCode).send(errorBody(error.code, error.message, error.details));
    }
    if (error instanceof AppError) {
      // Expected business outcomes (sold out, not found, ...) are not server faults.
      request.log.info({ code: error.code }, error.message);
      return reply.code(error.statusCode).send(errorBody(error.code, error.message));
    }
    const status = (error as FastifyError).statusCode;
    if (status !== undefined && status >= 400 && status < 500) {
      // Fastify's own client errors: malformed JSON, payload too large, wrong content type...
      return reply.code(status).send(errorBody('BAD_REQUEST', error.message));
    }
    request.log.error({ err: error }, 'unhandled error');
    return reply.code(500).send(errorBody('INTERNAL_ERROR', 'Internal server error'));
  });

  app.setNotFoundHandler((request, reply) =>
    reply.code(404).send(errorBody('NOT_FOUND', `Route ${request.method} ${request.url} not found`)),
  );

  // --- Routes ---
  await app.register(healthRoutes, { ctx });
  await app.register(eventRoutes, { ctx });
  await app.register(reservationRoutes, { ctx });
  await app.register(holdRoutes, { ctx });
  await app.register(waitlistRoutes, { ctx });
  await app.register(adminRoutes, { ctx });

  return app;
}
