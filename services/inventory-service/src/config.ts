import { z } from 'zod';

/**
 * Typed, validated environment configuration.
 *
 * Fails fast at startup on malformed values (e.g. PORT=abc) rather than letting
 * a bad config surface as a confusing runtime error later.
 *
 * `INVENTORY_PORT` / `INVENTORY_DATABASE_URL` are accepted as fallbacks because the
 * monorepo-level `.env` namespaces variables per service.
 */
const EnvSchema = z.object({
  PORT: z.coerce.number().int().min(1).max(65535).default(3001),
  DATABASE_URL: z.string().min(1).default('postgresql://seatlock:seatlock@localhost:5432/inventory'),
  KAFKA_BROKERS: z.string().min(1).default('localhost:9092'),
  REDIS_URL: z.string().min(1).default('redis://localhost:6379'),
  HOLD_TTL_SECONDS: z.coerce.number().int().positive().default(300),
  SERVICE_NAME: z.string().min(1).default('inventory-service'),
  LOG_LEVEL: z.enum(['fatal', 'error', 'warn', 'info', 'debug', 'trace', 'silent']).default('info'),
  DB_POOL_MAX: z.coerce.number().int().positive().default(20),
  MAX_TICKETS_PER_RESERVATION: z.coerce.number().int().positive().default(10),
  RATE_LIMIT_MAX: z.coerce.number().int().positive().default(20),
  RATE_LIMIT_WINDOW_SECONDS: z.coerce.number().int().positive().default(60),
  CACHE_TTL_SECONDS: z.coerce.number().int().positive().default(5),
  OUTBOX_POLL_INTERVAL_MS: z.coerce.number().int().positive().default(500),
  OUTBOX_BATCH_SIZE: z.coerce.number().int().positive().default(100),
  HOLD_EXPIRY_INTERVAL_MS: z.coerce.number().int().positive().default(5000),
  HOLD_EXPIRY_BATCH_SIZE: z.coerce.number().int().positive().default(100),
  CONSUMER_MAX_ATTEMPTS: z.coerce.number().int().positive().default(3),
  /** Set to "true" behind a load balancer so request.ip (rate limiting) uses X-Forwarded-For. */
  TRUST_PROXY: z
    .enum(['true', 'false'])
    .default('false')
    .transform((v) => v === 'true'),
});

export interface Config {
  port: number;
  databaseUrl: string;
  kafkaBrokers: string[];
  redisUrl: string;
  holdTtlSeconds: number;
  serviceName: string;
  logLevel: string;
  dbPoolMax: number;
  maxTicketsPerReservation: number;
  rateLimit: { max: number; windowSeconds: number };
  cacheTtlSeconds: number;
  outbox: { pollIntervalMs: number; batchSize: number };
  holdExpiry: { intervalMs: number; batchSize: number };
  consumerMaxAttempts: number;
  trustProxy: boolean;
}

export function loadConfig(env: NodeJS.ProcessEnv = process.env): Config {
  const parsed = EnvSchema.parse({
    ...env,
    PORT: env.PORT ?? env.INVENTORY_PORT,
    DATABASE_URL: env.DATABASE_URL ?? env.INVENTORY_DATABASE_URL,
  });

  return {
    port: parsed.PORT,
    databaseUrl: parsed.DATABASE_URL,
    kafkaBrokers: parsed.KAFKA_BROKERS.split(',')
      .map((b) => b.trim())
      .filter(Boolean),
    redisUrl: parsed.REDIS_URL,
    holdTtlSeconds: parsed.HOLD_TTL_SECONDS,
    serviceName: parsed.SERVICE_NAME,
    logLevel: parsed.LOG_LEVEL,
    dbPoolMax: parsed.DB_POOL_MAX,
    maxTicketsPerReservation: parsed.MAX_TICKETS_PER_RESERVATION,
    rateLimit: { max: parsed.RATE_LIMIT_MAX, windowSeconds: parsed.RATE_LIMIT_WINDOW_SECONDS },
    cacheTtlSeconds: parsed.CACHE_TTL_SECONDS,
    outbox: { pollIntervalMs: parsed.OUTBOX_POLL_INTERVAL_MS, batchSize: parsed.OUTBOX_BATCH_SIZE },
    holdExpiry: {
      intervalMs: parsed.HOLD_EXPIRY_INTERVAL_MS,
      batchSize: parsed.HOLD_EXPIRY_BATCH_SIZE,
    },
    consumerMaxAttempts: parsed.CONSUMER_MAX_ATTEMPTS,
    trustProxy: parsed.TRUST_PROXY,
  };
}

/** Process-wide config, read once at import time. */
export const config: Config = loadConfig();
