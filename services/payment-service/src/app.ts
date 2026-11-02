import cors from '@fastify/cors';
import Fastify from 'fastify';
import { AppError } from '@seatlock/shared';
import { ZodError } from 'zod';
import type { Logger } from './lib/logger.js';
import type { PaymentRepository } from './payments/types.js';
import { healthRoutes, type ReadinessCheck } from './routes/health.js';
import { paymentRoutes } from './routes/payments.js';

export interface AppDeps {
  logger: Logger;
  repo: PaymentRepository;
  producerName: string;
  enableSimulateRoute: boolean;
  readinessChecks: Record<string, ReadinessCheck>;
}

export async function buildApp(deps: AppDeps) {
  const app = Fastify({ loggerInstance: deps.logger });

  await app.register(cors, { origin: true });

  app.setErrorHandler((error: unknown, request, reply) => {
    if (error instanceof ZodError) {
      return reply.status(400).send({
        error: 'VALIDATION_ERROR',
        message: 'Validation failed',
        details: error.issues,
      });
    }
    if (error instanceof AppError) {
      return reply.status(error.statusCode).send({ error: error.code, message: error.message });
    }

    const statusCode =
      typeof (error as { statusCode?: unknown }).statusCode === 'number'
        ? (error as { statusCode: number }).statusCode
        : 500;
    if (statusCode >= 500) {
      request.log.error({ err: error }, 'Unhandled error');
      return reply.status(500).send({ error: 'INTERNAL_ERROR', message: 'Internal server error' });
    }
    return reply
      .status(statusCode)
      .send({ error: 'BAD_REQUEST', message: error instanceof Error ? error.message : 'Bad request' });
  });

  await app.register(healthRoutes, { checks: deps.readinessChecks });
  await app.register(paymentRoutes, {
    repo: deps.repo,
    producerName: deps.producerName,
    enableSimulateRoute: deps.enableSimulateRoute,
  });

  return app;
}
