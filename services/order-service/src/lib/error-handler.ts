import type { FastifyError, FastifyReply, FastifyRequest } from 'fastify';
import { AppError, ValidationError } from '@seatlock/shared';
import { ZodError } from 'zod';

interface ErrorBody {
  error: { code: string; message: string; details?: unknown };
}

/**
 * One place that turns exceptions into the API's uniform error shape:
 *   { "error": { "code": "...", "message": "...", "details"?: ... } }
 *
 * - Business errors (AppError subclasses from @seatlock/shared) carry their own status + code.
 * - Zod errors become 400s with field-level details.
 * - Fastify's own 4xx errors (bad JSON, payload too large) are passed through.
 * - Everything else is a 500 whose internals are logged but never leaked to the client.
 */
export function errorHandler(
  error: FastifyError | Error,
  request: FastifyRequest,
  reply: FastifyReply,
): FastifyReply {
  if (error instanceof ZodError) {
    const body: ErrorBody = {
      error: {
        code: 'VALIDATION_ERROR',
        message: 'Request validation failed',
        details: error.issues.map((i) => ({ path: i.path.join('.'), message: i.message })),
      },
    };
    return reply.status(400).send(body);
  }

  if (error instanceof AppError) {
    request.log.warn({ code: error.code, statusCode: error.statusCode }, error.message);
    const body: ErrorBody = { error: { code: error.code, message: error.message } };
    if (error instanceof ValidationError && error.details !== undefined) {
      body.error.details = error.details;
    }
    return reply.status(error.statusCode).send(body);
  }

  const fastifyError = error as FastifyError;
  if (fastifyError.statusCode && fastifyError.statusCode >= 400 && fastifyError.statusCode < 500) {
    return reply.status(fastifyError.statusCode).send({
      error: { code: fastifyError.code ?? 'BAD_REQUEST', message: fastifyError.message },
    } satisfies ErrorBody);
  }

  request.log.error({ err: error }, 'unhandled error');
  return reply.status(500).send({
    error: { code: 'INTERNAL_ERROR', message: 'Internal server error' },
  } satisfies ErrorBody);
}
