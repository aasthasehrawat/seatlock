import { z } from 'zod';

const EnvSchema = z.object({
  NODE_ENV: z.enum(['development', 'test', 'production']).default('development'),
  PORT: z.coerce.number().int().min(0).max(65535).default(3004),
  HOST: z.string().default('0.0.0.0'),
  DATABASE_URL: z
    .string()
    .min(1)
    .default('postgresql://seatlock:seatlock@localhost:5435/notifications'),
  KAFKA_BROKERS: z.string().min(1).default('localhost:9092'),
  KAFKA_CLIENT_ID: z.string().min(1).default('notification-service'),
  KAFKA_GROUP_ID: z.string().min(1).default('notification-service'),
  /** How many times to look up a hold's owner before giving up (events may arrive out of order). */
  USER_LOOKUP_RETRIES: z.coerce.number().int().min(0).max(20).default(5),
  USER_LOOKUP_DELAY_MS: z.coerce.number().int().min(0).default(250),
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
  userLookupRetries: number;
  userLookupDelayMs: number;
  logLevel: string;
  serviceName: string;
}

export const SERVICE_NAME = 'notification-service';

/**
 * Parse and validate configuration from environment variables.
 * The plain names (PORT, DATABASE_URL) win; the prefixed names from the root
 * `.env.example` (NOTIFICATION_PORT, NOTIFICATION_DATABASE_URL) are fallbacks.
 */
export function loadConfig(env: NodeJS.ProcessEnv = process.env): Config {
  const merged = {
    ...env,
    PORT: env.PORT ?? env.NOTIFICATION_PORT,
    DATABASE_URL: env.DATABASE_URL ?? env.NOTIFICATION_DATABASE_URL,
  };

  const parsed = EnvSchema.safeParse(merged);
  if (!parsed.success) {
    const issues = parsed.error.issues.map((i) => `${i.path.join('.')}: ${i.message}`).join('; ');
    throw new Error(`Invalid notification-service configuration: ${issues}`);
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
    userLookupRetries: e.USER_LOOKUP_RETRIES,
    userLookupDelayMs: e.USER_LOOKUP_DELAY_MS,
    logLevel: e.LOG_LEVEL,
    serviceName: SERVICE_NAME,
  };
}
