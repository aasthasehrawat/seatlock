import { describe, expect, it } from 'vitest';
import { EVENT_TYPES, createEnvelope, createId } from '@seatlock/shared';
import { PoisonMessageError } from '../lib/errors.js';
import { createHarness, inventoryReserved, type Harness } from '../testing/fixtures.js';
import { createDispatcher, parseMessage } from './message-handler.js';

const toBuffer = (value: unknown): Buffer => Buffer.from(JSON.stringify(value), 'utf8');

function envelopeFor(type: string, payload: unknown): Buffer {
  return toBuffer({ ...createEnvelope({ type: type as never, payload, producer: 'test' }) });
}

describe('parseMessage', () => {
  it('extracts routing metadata from a valid envelope', () => {
    const payload = inventoryReserved();
    const parsed = parseMessage(envelopeFor(EVENT_TYPES.InventoryReserved, payload));

    expect(parsed.type).toBe('InventoryReserved');
    expect(parsed.payload).toEqual(payload);
    expect(parsed.meta.eventId).toMatch(/^[0-9a-f-]{36}$/);
    expect(parsed.meta.correlationId).toBeTruthy();
  });

  it.each([
    ['empty value', null],
    ['non-JSON bytes', Buffer.from('not json')],
    ['JSON without an envelope shape', toBuffer({ hello: 'world' })],
    ['an eventId that is not a UUID', toBuffer({ eventId: 'x', type: 'T', correlationId: 'c', payload: {} })],
  ])('treats %s as poison (DLQ, never retried)', (_name, value) => {
    expect(() => parseMessage(value)).toThrow(PoisonMessageError);
  });
});

describe('createDispatcher', () => {
  const dispatchWith = (h: Harness) => createDispatcher(h.service);

  it('routes InventoryReserved to order creation', async () => {
    const h = createHarness();
    const message = parseMessage(envelopeFor(EVENT_TYPES.InventoryReserved, inventoryReserved()));

    const result = await dispatchWith(h)(message);

    expect(result.outcome).toBe('applied');
    expect(h.uow.orders).toHaveLength(1);
  });

  it('ignores event types it does not handle instead of dead-lettering them', async () => {
    const h = createHarness();
    const message = parseMessage(
      envelopeFor(EVENT_TYPES.NotificationRequested, { whatever: true }),
    );

    const result = await dispatchWith(h)(message);

    expect(result).toMatchObject({ outcome: 'ignored', reason: 'unhandled_event_type_NotificationRequested' });
  });

  it('ignores PaymentRequested seen on payments.events (that is our own command)', async () => {
    const h = createHarness();
    const message = parseMessage(envelopeFor(EVENT_TYPES.PaymentRequested, { paymentId: createId() }));

    const result = await dispatchWith(h)(message);

    expect(result.outcome).toBe('ignored');
  });

  it('dead-letters a known event type whose payload violates its schema', async () => {
    const h = createHarness();
    const message = parseMessage(
      envelopeFor(EVENT_TYPES.InventoryReserved, { holdId: 'not-a-uuid' }),
    );

    await expect(dispatchWith(h)(message)).rejects.toBeInstanceOf(PoisonMessageError);
    expect(h.uow.orders).toHaveLength(0);
  });
});