import { InvalidStateTransitionError } from '../errors.js';

export const ORDER_STATES = {
  PENDING_PAYMENT: 'PENDING_PAYMENT',
  CONFIRMED: 'CONFIRMED',
  FAILED: 'FAILED',
  EXPIRED: 'EXPIRED',
} as const;

export type OrderState = (typeof ORDER_STATES)[keyof typeof ORDER_STATES];

/** Only PENDING_PAYMENT orders can move; every other state is terminal. */
const ORDER_TRANSITIONS: Record<OrderState, readonly OrderState[]> = {
  PENDING_PAYMENT: ['CONFIRMED', 'FAILED', 'EXPIRED'],
  CONFIRMED: [],
  FAILED: [],
  EXPIRED: [],
};

export function canTransitionOrder(from: OrderState, to: OrderState): boolean {
  return ORDER_TRANSITIONS[from].includes(to);
}

export function assertOrderTransition(from: OrderState, to: OrderState): void {
  if (!canTransitionOrder(from, to)) {
    throw new InvalidStateTransitionError('order', from, to);
  }
}
