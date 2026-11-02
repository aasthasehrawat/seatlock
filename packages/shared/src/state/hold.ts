import { InvalidStateTransitionError } from '../errors.js';

export const HOLD_STATES = {
  ACTIVE: 'ACTIVE',
  CONFIRMED: 'CONFIRMED',
  EXPIRED: 'EXPIRED',
  RELEASED: 'RELEASED',
} as const;

export type HoldState = (typeof HOLD_STATES)[keyof typeof HOLD_STATES];

/** Only ACTIVE holds can move; every other state is terminal. */
const HOLD_TRANSITIONS: Record<HoldState, readonly HoldState[]> = {
  ACTIVE: ['CONFIRMED', 'EXPIRED', 'RELEASED'],
  CONFIRMED: [],
  EXPIRED: [],
  RELEASED: [],
};

export function canTransitionHold(from: HoldState, to: HoldState): boolean {
  return HOLD_TRANSITIONS[from].includes(to);
}

export function assertHoldTransition(from: HoldState, to: HoldState): void {
  if (!canTransitionHold(from, to)) {
    throw new InvalidStateTransitionError('hold', from, to);
  }
}
