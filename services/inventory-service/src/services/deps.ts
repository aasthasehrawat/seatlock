import type { TxPool } from '../db/pool.js';

/**
 * Explicit dependencies for the domain services.
 *
 * Passing these in (instead of importing singletons) keeps the services free of hidden global
 * state: unit tests hand in a mocked pool, integration tests hand in a Testcontainers pool,
 * production wires the real one in app.ts.
 */
export interface ServiceDeps {
  pool: TxPool;
  holdTtlSeconds: number;
  /** Stamped into event envelopes as `producer`. */
  serviceName: string;
}
