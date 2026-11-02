import { Redis } from 'ioredis';
import { config } from '../config.js';
import { logger } from './logger.js';

/**
 * Redis is an OPTIMIZATION here (read cache + rate limiter), never a source of truth.
 * Every helper below fails OPEN: if Redis is down we serve from Postgres and skip rate
 * limiting rather than turning a cache outage into a ticket-sales outage.
 * Correctness of inventory lives entirely in Postgres.
 */
let client: Redis | undefined;

export function createRedis(url: string = config.redisUrl): Redis {
  const redis = new Redis(url, {
    lazyConnect: true,
    // Fail fast instead of queueing commands while disconnected.
    enableOfflineQueue: false,
    maxRetriesPerRequest: 1,
    commandTimeout: 250,
    retryStrategy: (times) => Math.min(times * 200, 5_000),
  });
  redis.on('error', (err: Error) => logger.warn({ err: err.message }, 'redis error'));
  return redis;
}

/** Process-wide Redis singleton (connection is established lazily by `connectRedis`). */
export function getRedis(): Redis {
  client ??= createRedis();
  return client;
}

/** Best-effort connect; the service starts even if Redis is unreachable. */
export async function connectRedis(redis: Redis = getRedis()): Promise<void> {
  try {
    await redis.connect();
  } catch (err) {
    logger.warn({ err: (err as Error).message }, 'redis unavailable at startup; will keep retrying');
  }
}

export async function disconnectRedis(redis: Redis | undefined = client): Promise<void> {
  if (!redis) return;
  try {
    await redis.quit();
  } catch {
    redis.disconnect();
  }
}

// ---------------------------------------------------------------------------
// Read-through JSON cache
// ---------------------------------------------------------------------------

/** Concurrent misses for the same key share one DB query (in-process stampede guard). */
const inflightLoads = new Map<string, Promise<unknown>>();

export interface CacheResult<T> {
  value: T;
  hit: boolean;
}

export async function getOrSetJson<T>(
  redis: Redis | null,
  key: string,
  ttlSeconds: number,
  loader: () => Promise<T>,
): Promise<CacheResult<T>> {
  if (redis) {
    try {
      const cached = await redis.get(key);
      if (cached !== null) return { value: JSON.parse(cached) as T, hit: true };
    } catch (err) {
      logger.warn({ err: (err as Error).message, key }, 'cache read failed; falling back to db');
    }
  }

  let load = inflightLoads.get(key) as Promise<T> | undefined;
  if (!load) {
    load = loader().finally(() => inflightLoads.delete(key));
    inflightLoads.set(key, load);
  }
  const value = await load;

  if (redis) {
    try {
      await redis.set(key, JSON.stringify(value), 'EX', ttlSeconds);
    } catch (err) {
      logger.warn({ err: (err as Error).message, key }, 'cache write failed');
    }
  }
  return { value, hit: false };
}

// ---------------------------------------------------------------------------
// Fixed-window rate limiter
// ---------------------------------------------------------------------------

/**
 * INCR + EXPIRE must be atomic: if the process died between them, the key would live
 * forever and the client would be blocked permanently. A Lua script runs atomically.
 * Returns {count, ttlSeconds}.
 */
const RATE_LIMIT_LUA = `
local count = redis.call('INCR', KEYS[1])
if count == 1 then
  redis.call('EXPIRE', KEYS[1], ARGV[1])
end
local ttl = redis.call('TTL', KEYS[1])
return {count, ttl}
`;

export interface RateLimitResult {
  allowed: boolean;
  limit: number;
  remaining: number;
  retryAfterSeconds: number;
}

export async function checkRateLimit(
  redis: Redis | null,
  key: string,
  limit: number,
  windowSeconds: number,
): Promise<RateLimitResult> {
  const open: RateLimitResult = { allowed: true, limit, remaining: limit, retryAfterSeconds: 0 };
  if (!redis) return open;

  try {
    const [count, ttl] = (await redis.eval(RATE_LIMIT_LUA, 1, key, windowSeconds)) as [number, number];
    const retryAfterSeconds = ttl > 0 ? ttl : windowSeconds;
    return {
      allowed: count <= limit,
      limit,
      remaining: Math.max(0, limit - count),
      retryAfterSeconds,
    };
  } catch (err) {
    logger.warn({ err: (err as Error).message }, 'rate limiter unavailable; failing open');
    return open;
  }
}
