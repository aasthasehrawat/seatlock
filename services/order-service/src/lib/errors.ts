import { AppError } from '@seatlock/shared';

/**
 * Thrown by message processing when retrying cannot help (bad payload, broken invariant).
 * The Kafka consumer routes these straight to the DLQ instead of the retry topic.
 */
export class NonRetryableError extends Error {
  constructor(message: string) {
    super(message);
    this.name = new.target.name;
  }
}

/** The message could not be parsed or failed schema validation. Never worth retrying. */
export class PoisonMessageError extends NonRetryableError {}

/** The order is not in a state where payment can be started (e.g. already CONFIRMED). */
export class OrderNotPayableError extends AppError {
  constructor(orderId: string, status: string) {
    super(409, 'ORDER_NOT_PAYABLE', `Order ${orderId} is ${status} and cannot be paid`);
  }
}

/** A payment was already requested for this order under a different idempotency key. */
export class PaymentAlreadyInitiatedError extends AppError {
  constructor(orderId: string) {
    super(409, 'PAYMENT_ALREADY_INITIATED', `A payment is already in progress for order ${orderId}`);
  }
}
