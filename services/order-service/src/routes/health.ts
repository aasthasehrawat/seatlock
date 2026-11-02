import type { FastifyPluginAsync } from 'fastify';

export interface ReadinessCheck {
  name: string;
  check: () => Promise<boolean>;
}

export interface HealthRoutesOptions {
  checks: ReadinessCheck[];
  /** Per-check time budget. A hung dependency must not hang the probe. */
  timeoutMs?: number;
}

async function runCheck(
  { name, check }: ReadinessCheck,
  timeoutMs: number,
): Promise<[string, 'up' | 'down']> {
  let timer: NodeJS.Timeout | undefined;
  try {
    const timeout = new Promise<boolean>((resolve) => {
      timer = setTimeout(() => resolve(false), timeoutMs);
    });
    const ok = await Promise.race([check().catch(() => false), timeout]);
    return [name, ok ? 'up' : 'down'];
  } finally {
    clearTimeout(timer);
  }
}

/**
 * - /health/live:  "is the process up?"  Kubernetes restarts the pod if this fails.
 *                  Deliberately checks no dependencies: a DB outage must not cause restart storms.
 * - /health/ready: "can it serve traffic?" Failing removes the pod from the load balancer only.
 */
export const healthRoutes: FastifyPluginAsync<HealthRoutesOptions> = async (app, opts) => {
  const timeoutMs = opts.timeoutMs ?? 2000;

  app.get('/health/live', async () => ({ status: 'ok' }));

  app.get('/health/ready', async (_request, reply) => {
    const results = await Promise.all(opts.checks.map((c) => runCheck(c, timeoutMs)));
    const checks = Object.fromEntries(results);
    const ready = results.every(([, status]) => status === 'up');

    return reply.status(ready ? 200 : 503).send({ status: ready ? 'ready' : 'unavailable', checks });
  });
};
