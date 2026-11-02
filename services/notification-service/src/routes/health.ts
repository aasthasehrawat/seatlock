import type { FastifyPluginAsync } from 'fastify';

export type ReadinessCheck = () => Promise<boolean>;

export interface HealthRouteOptions {
  checks: Record<string, ReadinessCheck>;
}

export const healthRoutes: FastifyPluginAsync<HealthRouteOptions> = async (app, opts) => {
  // Liveness: the process is up and the event loop is responsive.
  app.get('/health/live', async () => ({ status: 'ok' }));

  // Readiness: dependencies are reachable; 503 tells the orchestrator to hold traffic.
  app.get('/health/ready', async (_request, reply) => {
    const results: Record<string, boolean> = {};
    for (const [name, check] of Object.entries(opts.checks)) {
      try {
        results[name] = await check();
      } catch {
        results[name] = false;
      }
    }
    const ready = Object.values(results).every(Boolean);
    return reply.status(ready ? 200 : 503).send({ status: ready ? 'ready' : 'not_ready', checks: results });
  });
};
