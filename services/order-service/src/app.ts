import cors from '@fastify/cors';
import Fastify from 'fastify';
import type { Logger } from './lib/logger.js';
import type { OrderAnalytics } from './db/analytics-repo.js';
import { errorHandler } from './lib/error-handler.js';
import { adminRoutes } from './routes/admin.js';
import { healthRoutes, type ReadinessCheck } from './routes/health.js';
import { orderRoutes } from './routes/orders.js';
import type { OrderService } from './services/order-service.js';

export interface AppDeps {
  logger: Logger;
  service: OrderService;
  /** Read-only dashboard queries for /admin/overview (optional so unit tests need no database). */
  analytics?: OrderAnalytics;
  readinessChecks?: ReadinessCheck[];
}

/**
 * Build the Fastify app without starting it. Keeping construction separate from `listen()` lets
 * tests drive it with `app.inject()` (no sockets) and fake dependencies.
 */
export async function buildApp(deps: AppDeps) {
  const app = Fastify({
    loggerInstance: deps.logger,
    requestIdHeader: 'x-request-id',
    // Small bodies only: this API accepts a tiny JSON object at most.
    bodyLimit: 16 * 1024,
  });

  await app.register(cors, { origin: true });
  app.setErrorHandler(errorHandler);
  app.setNotFoundHandler((request, reply) =>
    reply.status(404).send({
      error: { code: 'ROUTE_NOT_FOUND', message: `Route ${request.method} ${request.url} not found` },
    }),
  );

  await app.register(healthRoutes, { checks: deps.readinessChecks ?? [] });
  await app.register(orderRoutes, { service: deps.service });
  await app.register(adminRoutes, { analytics: deps.analytics });

  return app;
}
