import { describe, expect, it } from 'vitest';
import { loadConfig } from './config.js';

describe('loadConfig', () => {
  it('applies sensible defaults (port 3002)', () => {
    const config = loadConfig({});

    expect(config.port).toBe(3002);
    expect(config.kafkaBrokers).toEqual(['localhost:9092']);
    expect(config.enableWorkers).toBe(true);
    expect(config.defaultCurrency).toBe('USD');
    expect(config.retryDelayMs).toBe(5000);
  });

  it('accepts the monorepo-style prefixed variables', () => {
    const config = loadConfig({
      ORDER_PORT: '4100',
      ORDER_DATABASE_URL: 'postgresql://u:p@db:5432/orders',
    });

    expect(config.port).toBe(4100);
    expect(config.databaseUrl).toBe('postgresql://u:p@db:5432/orders');
  });

  it('prefers the unprefixed variables when both are set', () => {
    expect(loadConfig({ PORT: '5000', ORDER_PORT: '4100' }).port).toBe(5000);
  });

  it('parses lists and booleans', () => {
    const config = loadConfig({ KAFKA_BROKERS: 'a:9092, b:9092', ENABLE_WORKERS: 'false' });

    expect(config.kafkaBrokers).toEqual(['a:9092', 'b:9092']);
    expect(config.enableWorkers).toBe(false);
  });

  it('fails fast with a readable message on invalid input', () => {
    expect(() => loadConfig({ PORT: 'abc' })).toThrow(/Invalid order-service configuration[\s\S]*PORT/);
    expect(() => loadConfig({ DEFAULT_CURRENCY: 'DOLLARS' })).toThrow(/DEFAULT_CURRENCY/);
  });
});
