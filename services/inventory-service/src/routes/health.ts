import type { FastifyPluginAsync } from 'fastify';
import type { AppContext } from '../context.js';

const READINESS_TIMEOUT_MS = 2_000;

function withTimeout<T>(promise: Promise<T>, ms: number): Promise<T> {
  let timer: NodeJS.Timeout;
  const timeout = new Promise<never>((_, reject) => {
    timer = setTimeout(() => reject(new Error(`timed out after ${ms}ms`)), ms);
  });
  return Promise.race([promise, timeout]).finally(() => clearTimeout(timer));
}

/**
 * Liveness vs readiness (Kubernetes semantics):
 *  - live:  "the process is up". Never touches dependencies; failing it makes the orchestrator
 *           restart the pod, which would not fix a database outage.
 *  - ready: "I can serve traffic". Fails (503) if Postgres is unreachable so the load balancer
 *           stops routing here. Redis/Kafka are reported but do NOT fail readiness: the service
 *           degrades gracefully without them.
 */
export const healthRoutes: FastifyPluginAsync<{ ctx: AppContext }> = async (app, { ctx }) => {
  app.get('/health/live', async () => ({ status: 'ok' }));

  app.get('/health/ready', async (_request, reply) => {
    let database: 'up' | 'down' = 'up';
    try {
      await withTimeout(ctx.pool.query('SELECT 1'), READINESS_TIMEOUT_MS);
    } catch (err) {
      database = 'down';
      ctx.logger.error({ err }, 'readiness check: database unavailable');
    }

    const redis = ctx.redis?.status === 'ready' ? 'up' : ctx.redis ? 'down' : 'disabled';
    const ready = database === 'up';
    return reply.code(ready ? 200 : 503).send({
      status: ready ? 'ready' : 'unavailable',
      checks: { database, redis },
    });
  });
};
