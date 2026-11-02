import { describe, expect, it } from 'vitest';
import { InvalidStateTransitionError } from '../errors.js';
import { HOLD_STATES, assertHoldTransition, canTransitionHold, type HoldState } from './hold.js';

const ALL = Object.values(HOLD_STATES);

describe('hold state machine', () => {
  it.each(['CONFIRMED', 'EXPIRED', 'RELEASED'] as const)('allows ACTIVE -> %s', (to) => {
    expect(canTransitionHold('ACTIVE', to)).toBe(true);
    expect(() => assertHoldTransition('ACTIVE', to)).not.toThrow();
  });

  it.each(['CONFIRMED', 'EXPIRED', 'RELEASED'] as const)('treats %s as terminal', (from) => {
    for (const to of ALL) {
      expect(canTransitionHold(from, to)).toBe(false);
    }
  });

  it('does not allow ACTIVE -> ACTIVE', () => {
    expect(canTransitionHold('ACTIVE', 'ACTIVE')).toBe(false);
  });

  it('throws InvalidStateTransitionError with a 409 for illegal transitions', () => {
    const from: HoldState = 'EXPIRED';
    expect(() => assertHoldTransition(from, 'CONFIRMED')).toThrow(InvalidStateTransitionError);
    try {
      assertHoldTransition(from, 'CONFIRMED');
    } catch (err) {
      const e = err as InvalidStateTransitionError;
      expect(e.statusCode).toBe(409);
      expect(e.code).toBe('INVALID_STATE_TRANSITION');
      expect(e.message).toContain('EXPIRED -> CONFIRMED');
    }
  });
});
