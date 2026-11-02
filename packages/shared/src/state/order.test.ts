import { describe, expect, it } from 'vitest';
import { InvalidStateTransitionError } from '../errors.js';
import { ORDER_STATES, assertOrderTransition, canTransitionOrder, type OrderState } from './order.js';

const ALL = Object.values(ORDER_STATES);

describe('order state machine', () => {
  it.each(['CONFIRMED', 'FAILED', 'EXPIRED'] as const)('allows PENDING_PAYMENT -> %s', (to) => {
    expect(canTransitionOrder('PENDING_PAYMENT', to)).toBe(true);
    expect(() => assertOrderTransition('PENDING_PAYMENT', to)).not.toThrow();
  });

  it.each(['CONFIRMED', 'FAILED', 'EXPIRED'] as const)('treats %s as terminal', (from) => {
    for (const to of ALL) {
      expect(canTransitionOrder(from, to)).toBe(false);
    }
  });

  it('does not allow PENDING_PAYMENT -> PENDING_PAYMENT', () => {
    expect(canTransitionOrder('PENDING_PAYMENT', 'PENDING_PAYMENT')).toBe(false);
  });

  it('throws InvalidStateTransitionError with a 409 for illegal transitions', () => {
    const from: OrderState = 'FAILED';
    expect(() => assertOrderTransition(from, 'CONFIRMED')).toThrow(InvalidStateTransitionError);
    try {
      assertOrderTransition(from, 'CONFIRMED');
    } catch (err) {
      const e = err as InvalidStateTransitionError;
      expect(e.statusCode).toBe(409);
      expect(e.code).toBe('INVALID_STATE_TRANSITION');
      expect(e.message).toContain('FAILED -> CONFIRMED');
    }
  });
});
