import { describe, expect, it } from 'vitest';
import { loadConfig } from './config.js';

describe('loadConfig', () => {
  it('applies defaults', () => {
    const config = loadConfig({});
    expect(config.port).toBe(3004);
    expect(config.kafkaBrokers).toEqual(['localhost:9092']);
    expect(config.kafkaGroupId).toBe('notification-service');
  });

  it('reads overrides and splits brokers', () => {
    const config = loadConfig({ PORT: '4004', DATABASE_URL: 'postgresql://x/y', KAFKA_BROKERS: 'a:1, b:2' });
    expect(config.port).toBe(4004);
    expect(config.databaseUrl).toBe('postgresql://x/y');
    expect(config.kafkaBrokers).toEqual(['a:1', 'b:2']);
  });

  it('falls back to the prefixed variables from the root .env', () => {
    const config = loadConfig({ NOTIFICATION_PORT: '3999', NOTIFICATION_DATABASE_URL: 'postgresql://p/q' });
    expect(config.port).toBe(3999);
    expect(config.databaseUrl).toBe('postgresql://p/q');
  });

  it('rejects invalid values', () => {
    expect(() => loadConfig({ PORT: 'abc' })).toThrow(/PORT/);
  });
});
