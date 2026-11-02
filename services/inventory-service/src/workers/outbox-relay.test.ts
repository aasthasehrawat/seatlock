import { TOPICS, createEnvelope, createId } from '@seatlock/shared';
import { describe, expect, it, vi } from 'vitest';
import { createMockPool } from '../test-support/mock-pool.js';
import { relayOnce } from './outbox-relay.js';

const silentLogger = { info() {}, warn() {}, error() {}, debug() {} } as never;

function outboxRow(concertId: string) {
  const holdId = createId();
  const payload = createEnvelope({
    type: 'HoldExpired',
    payload: { holdId, eventId: concertId, quantity: 2 },
    producer: 'inventory-service',
    correlationId: 'corr-9',
  });
  return {
    id: createId(),
    aggregate_type: 'hold',
    aggregate_id: holdId,
    event_type: 'HoldExpired',
    payload,
    headers: { correlationId: 'corr-9', eventId: concertId, type: 'HoldExpired', messageId: payload.eventId },
  };
}

describe('outbox relay', () => {
  it('publishes claimed rows keyed by concert id, then marks them published in the same transaction', async () => {
    const concertId = createId();
    const rows = [outboxRow(concertId), outboxRow(concertId)];
    const t = createMockPool((sql) => (sql.startsWith('SELECT id, aggregate_type') ? { rows } : undefined));
    const send = vi.fn(async () => []);

    const count = await relayOnce({ pool: t.pool, producer: async () => ({ send }), batchSize: 100, logger: silentLogger });

    expect(count).toBe(2);
    expect(send).toHaveBeenCalledTimes(1);
    const record = (send.mock.calls[0] as unknown as [{ topic: string; acks: number; messages: Array<Record<string, any>> }])[0];
    expect(record.topic).toBe(TOPICS.INVENTORY);
    expect(record.acks).toBe(-1);
    expect(record.messages).toHaveLength(2);
    expect(record.messages[0]).toMatchObject({
      key: concertId, // partition key = concert id
      headers: { correlationId: 'corr-9', eventId: concertId, type: 'HoldExpired' },
    });
    expect(JSON.parse(record.messages[0]!.value as string)).toEqual(rows[0]!.payload);

    // Claim uses SKIP LOCKED so several relays can cooperate; mark happens before COMMIT.
    expect(t.find('SELECT id, aggregate_type')[0]!.sql).toContain('FOR UPDATE SKIP LOCKED');
    const order = t.calls.map((c) => c.sql.split(' ')[0]);
    expect(order.indexOf('UPDATE')).toBeGreaterThan(order.indexOf('SELECT'));
    expect(order.indexOf('UPDATE')).toBeLessThan(order.indexOf('COMMIT'));
    expect(t.find('UPDATE outbox')[0]!.values).toEqual([rows.map((r) => r.id)]);
  });

  it('leaves rows unpublished (ROLLBACK, no UPDATE) when Kafka rejects the batch', async () => {
    const rows = [outboxRow(createId())];
    const t = createMockPool((sql) => (sql.startsWith('SELECT id, aggregate_type') ? { rows } : undefined));
    const send = vi.fn(async () => {
      throw new Error('broker unavailable');
    });

    await expect(
      relayOnce({ pool: t.pool, producer: async () => ({ send }), batchSize: 100, logger: silentLogger }),
    ).rejects.toThrow('broker unavailable');

    expect(t.has('UPDATE outbox')).toBe(false);
    expect(t.has('ROLLBACK')).toBe(true);
    expect(t.has('COMMIT')).toBe(false);
  });

  it('does nothing (and does not even connect the producer) when the outbox is empty', async () => {
    const t = createMockPool(() => undefined);
    const producer = vi.fn(async () => ({ send: vi.fn() }));

    expect(await relayOnce({ pool: t.pool, producer, batchSize: 100, logger: silentLogger })).toBe(0);
    expect(producer).not.toHaveBeenCalled();
    expect(t.has('UPDATE outbox')).toBe(false);
  });
});
