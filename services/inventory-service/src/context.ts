import type { Redis } from 'ioredis';
import type { Pool } from 'pg';
import type { Logger } from 'pino';
import type { Config } from './config.js';
import type { ServiceDeps } from './services/deps.js';

/** Everything the HTTP layer needs, assembled once in index.ts and injected into routes. */
export interface AppContext {
  config: Config;
  pool: Pool;
  /** null disables caching + rate limiting (e.g. in tests). */
  redis: Redis | null;
  services: ServiceDeps;
  logger: Logger;
}
