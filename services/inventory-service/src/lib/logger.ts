import { pino, type Logger } from 'pino';
import { config } from '../config.js';
import { currentCorrelationId } from './correlation.js';

/**
 * Structured JSON logger. The mixin stamps every line with the correlation id of the
 * current async context (set per HTTP request / Kafka message), so a single user flow
 * can be grepped across services.
 */
export const logger: Logger = pino({
  level: config.logLevel,
  base: { service: config.serviceName },
  timestamp: pino.stdTimeFunctions.isoTime,
  mixin() {
    const correlationId = currentCorrelationId();
    return correlationId ? { correlationId } : {};
  },
});

export type { Logger };
