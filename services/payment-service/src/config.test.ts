import { describe, expect, it } from 'vitest';
import { loadConfig } from './config.js';

describe('loadConfig', () => {
  it('applies defaults', () => {
    const config = loadConfig({});
    expect(config.port).toBe(3003);
    expect(config.paymentFailureRate).toBe(0.2);
    expect(config.paymentLatencyMs).toBe(500);
    expect(config.kafkaBrokers).toEqual(['localhost:9092']);
    expect(config.enableSimulateRoute).toBe(true);
  });

  it('reads overrides and splits brokers', () => {
    const config = loadConfig({
      PORT: '4000',
      DATABASE_URL: 'postgresql://x/y',
      KAFKA_BROKERS: 'a:1, b:2',
      PAYMENT_FAILURE_RATE: '0.5',
      PAYMENT_LATENCY_MS: '10',
    });
    expect(config.port).toBe(4000);
    expect(config.databaseUrl).toBe('postgresql://x/y');
    expect(config.kafkaBrokers).toEqual(['a:1', 'b:2']);
    expect(config.paymentFailureRate).toBe(0.5);
    expect(config.paymentLatencyMs).toBe(10);
  });

  it('falls back to the prefixed variables from the root .env', () => {
    const config = loadConfig({ PAYMENT_PORT: '3999', PAYMENT_DATABASE_URL: 'postgresql://p/q' });
    expect(config.port).toBe(3999);
    expect(config.databaseUrl).toBe('postgresql://p/q');
  });

  it('disables the simulate route in production unless explicitly enabled', () => {
    expect(loadConfig({ NODE_ENV: 'production' }).enableSimulateRoute).toBe(false);
    expect(loadConfig({ NODE_ENV: 'production', ENABLE_SIMULATE_ROUTE: 'true' }).enableSimulateRoute).toBe(true);
  });

  it('rejects an out-of-range failure rate', () => {
    expect(() => loadConfig({ PAYMENT_FAILURE_RATE: '1.5' })).toThrow(/PAYMENT_FAILURE_RATE/);
  });
});
