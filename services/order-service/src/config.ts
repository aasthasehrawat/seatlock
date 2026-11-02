import { z } from 'zod';

/** "true"/"false"/"1"/"0" -> boolean, with a default applied before parsing. */
const flag = (defaultValue: 'true' | 'false') =>
  z
    .enum(['true', 'false', '1', '0'])
    .default(defaultValue)
    .transform((v) => v === 'true' || v === '1');

const positiveInt = (defaultValue: number) => z.coerce.number().int().positive().default(defaultValue);

const EnvSchema = z.object({
  NODE_ENV: z.enum(['development', 'test', 'production']).default('development'),
  SERVICE_NAME: z.string().min(1).default('order-service'),
  HOST: z.string().min(1).default('0.0.0.0'),
  PORT: z.coerce.number().int().min(1).max(65535).default(3002),
  LOG_LEVEL: z.enum(['fatal', 'error', 'warn', 'info', 'debug', 'trace', 'silent']).default('info'),

  DATABASE_URL: z.string().min(1).default('postgresql://seatlock:seatlock@localhost:5433/orders'),
  DB_POOL_MAX: positiveInt(10),
  RUN_MIGRATIONS_ON_START: flag('true'),

  KAFKA_BROKERS: z.string().min(1).default('localhost:9092'),
  KAFKA_CLIENT_ID: z.string().min(1).default('order-service'),
  KAFKA_GROUP_ID: z.string().min(1).default('order-service'),
  KAFKA_FROM_BEGINNING: flag('true'),
  KAFKA_ENSURE_TOPICS: flag('true'),
  KAFKA_TOPIC_PARTITIONS: positiveInt(3),

  /** Disable to run the HTTP API only (no consumers, no outbox relay). */
  ENABLE_WORKERS: flag('true'),

  OUTBOX_POLL_INTERVAL_MS: positiveInt(500),
  OUTBOX_BATCH_SIZE: positiveInt(100),
  OUTBOX_RETENTION_HOURS: positiveInt(24),
  IDEMPOTENCY_TTL_HOURS: positiveInt(24),

  RETRY_DELAY_MS: positiveInt(5000),
  MAX_RETRY_ATTEMPTS: positiveInt(5),

  DEFAULT_CURRENCY: z
    .string()
    .regex(/^[A-Za-z]{3}$/)
    .default('USD')
    .transform((v) => v.toUpperCase()),
});

export interface Config {
  env: 'development' | 'test' | 'production';
  serviceName: string;
  host: string;
  port: number;
  logLevel: 'fatal' | 'error' | 'warn' | 'info' | 'debug' | 'trace' | 'silent';

  databaseUrl: string;
  dbPoolMax: number;
  runMigrationsOnStart: boolean;

  kafkaBrokers: string[];
  kafkaClientId: string;
  kafkaGroupId: string;
  kafkaFromBeginning: boolean;
  kafkaEnsureTopics: boolean;
  kafkaTopicPartitions: number;

  enableWorkers: boolean;

  outboxPollIntervalMs: number;
  outboxBatchSize: number;
  outboxRetentionHours: number;
  idempotencyTtlHours: number;

  retryDelayMs: number;
  maxRetryAttempts: number;

  defaultCurrency: string;
}

/**
 * Parse and validate configuration from environment variables. Fails fast with a readable
 * message so a misconfigured pod crashes at boot instead of misbehaving later.
 *
 * The monorepo's root `.env.example` uses service-prefixed names (ORDER_PORT,
 * ORDER_DATABASE_URL); those are accepted as fallbacks for the unprefixed names.
 */
export function loadConfig(env: NodeJS.ProcessEnv = process.env): Config {
  const parsed = EnvSchema.safeParse({
    ...env,
    PORT: env.PORT ?? env.ORDER_PORT,
    DATABASE_URL: env.DATABASE_URL ?? env.ORDER_DATABASE_URL,
  });

  if (!parsed.success) {
    const problems = parsed.error.issues
      .map((issue) => `  - ${issue.path.join('.') || '(root)'}: ${issue.message}`)
      .join('\n');
    throw new Error(`Invalid order-service configuration:\n${problems}`);
  }

  const e = parsed.data;
  return {
    env: e.NODE_ENV,
    serviceName: e.SERVICE_NAME,
    host: e.HOST,
    port: e.PORT,
    logLevel: e.LOG_LEVEL,

    databaseUrl: e.DATABASE_URL,
    dbPoolMax: e.DB_POOL_MAX,
    runMigrationsOnStart: e.RUN_MIGRATIONS_ON_START,

    kafkaBrokers: e.KAFKA_BROKERS.split(',')
      .map((b) => b.trim())
      .filter(Boolean),
    kafkaClientId: e.KAFKA_CLIENT_ID,
    kafkaGroupId: e.KAFKA_GROUP_ID,
    kafkaFromBeginning: e.KAFKA_FROM_BEGINNING,
    kafkaEnsureTopics: e.KAFKA_ENSURE_TOPICS,
    kafkaTopicPartitions: e.KAFKA_TOPIC_PARTITIONS,

    enableWorkers: e.ENABLE_WORKERS,

    outboxPollIntervalMs: e.OUTBOX_POLL_INTERVAL_MS,
    outboxBatchSize: e.OUTBOX_BATCH_SIZE,
    outboxRetentionHours: e.OUTBOX_RETENTION_HOURS,
    idempotencyTtlHours: e.IDEMPOTENCY_TTL_HOURS,

    retryDelayMs: e.RETRY_DELAY_MS,
    maxRetryAttempts: e.MAX_RETRY_ATTEMPTS,

    defaultCurrency: e.DEFAULT_CURRENCY,
  };
}
