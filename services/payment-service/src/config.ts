import { z } from 'zod';

const booleanFromString = z
  .enum(['true', 'false', '1', '0'])
  .transform((value) => value === 'true' || value === '1');

const EnvSchema = z.object({
  NODE_ENV: z.enum(['development', 'test', 'production']).default('development'),
  PORT: z.coerce.number().int().min(0).max(65535).default(3003),
  HOST: z.string().default('0.0.0.0'),
  DATABASE_URL: z.string().min(1).default('postgresql://seatlock:seatlock@localhost:5434/payments'),
  KAFKA_BROKERS: z.string().min(1).default('localhost:9092'),
  KAFKA_CLIENT_ID: z.string().min(1).default('payment-service'),
  KAFKA_GROUP_ID: z.string().min(1).default('payment-service'),
  PAYMENT_FAILURE_RATE: z.coerce.number().min(0).max(1).default(0.2),
  PAYMENT_LATENCY_MS: z.coerce.number().int().min(0).default(500),
  OUTBOX_POLL_INTERVAL_MS: z.coerce.number().int().min(10).default(500),
  OUTBOX_BATCH_SIZE: z.coerce.number().int().min(1).max(1000).default(50),
  CONSUMER_CONCURRENCY: z.coerce.number().int().min(1).max(50).default(3),
  ENABLE_SIMULATE_ROUTE: booleanFromString.optional(),
  LOG_LEVEL: z.enum(['fatal', 'error', 'warn', 'info', 'debug', 'trace', 'silent']).default('info'),
});

export interface Config {
  nodeEnv: 'development' | 'test' | 'production';
  port: number;
  host: string;
  databaseUrl: string;
  kafkaBrokers: string[];
  kafkaClientId: string;
  kafkaGroupId: string;
  paymentFailureRate: number;
  paymentLatencyMs: number;
  outboxPollIntervalMs: number;
  outboxBatchSize: number;
  consumerConcurrency: number;
  /** The debug simulate route is on by default, except in production. */
  enableSimulateRoute: boolean;
  logLevel: string;
  serviceName: string;
}

export const SERVICE_NAME = 'payment-service';

/**
 * Parse and validate configuration from environment variables.
 * Throws a readable error if anything is invalid.
 *
 * The plain names (PORT, DATABASE_URL) win; the prefixed names from the root
 * `.env.example` (PAYMENT_PORT, PAYMENT_DATABASE_URL) are accepted as fallbacks.
 */
export function loadConfig(env: NodeJS.ProcessEnv = process.env): Config {
  const merged = {
    ...env,
    PORT: env.PORT ?? env.PAYMENT_PORT,
    DATABASE_URL: env.DATABASE_URL ?? env.PAYMENT_DATABASE_URL,
  };

  const parsed = EnvSchema.safeParse(merged);
  if (!parsed.success) {
    const issues = parsed.error.issues.map((i) => `${i.path.join('.')}: ${i.message}`).join('; ');
    throw new Error(`Invalid payment-service configuration: ${issues}`);
  }
  const e = parsed.data;

  return {
    nodeEnv: e.NODE_ENV,
    port: e.PORT,
    host: e.HOST,
    databaseUrl: e.DATABASE_URL,
    kafkaBrokers: e.KAFKA_BROKERS.split(',')
      .map((b) => b.trim())
      .filter(Boolean),
    kafkaClientId: e.KAFKA_CLIENT_ID,
    kafkaGroupId: e.KAFKA_GROUP_ID,
    paymentFailureRate: e.PAYMENT_FAILURE_RATE,
    paymentLatencyMs: e.PAYMENT_LATENCY_MS,
    outboxPollIntervalMs: e.OUTBOX_POLL_INTERVAL_MS,
    outboxBatchSize: e.OUTBOX_BATCH_SIZE,
    consumerConcurrency: e.CONSUMER_CONCURRENCY,
    enableSimulateRoute: e.ENABLE_SIMULATE_ROUTE ?? e.NODE_ENV !== 'production',
    logLevel: e.LOG_LEVEL,
    serviceName: SERVICE_NAME,
  };
}
