import pino, { type Logger } from 'pino';
import type { Config } from '../config.js';

export type { Logger };

/** Structured JSON logs in production; human-readable output while developing. */
export function createLogger(config: Pick<Config, 'logLevel' | 'serviceName' | 'env'>): Logger {
  return pino({
    level: config.logLevel,
    base: { service: config.serviceName },
    timestamp: pino.stdTimeFunctions.isoTime,
    ...(config.env === 'development'
      ? { transport: { target: 'pino-pretty', options: { colorize: true } } }
      : {}),
  });
}
