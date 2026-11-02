/** Base class for all expected (business) errors. Carries an HTTP status and a stable code. */
export class AppError extends Error {
  constructor(
    public readonly statusCode: number,
    public readonly code: string,
    message: string,
  ) {
    super(message);
    this.name = new.target.name;
  }
}

export class SoldOutError extends AppError {
  constructor(message = 'Not enough tickets available') {
    super(409, 'SOLD_OUT', message);
  }
}

export class IdempotencyConflictError extends AppError {
  constructor(message = 'Idempotency key was already used with a different request') {
    super(409, 'IDEMPOTENCY_CONFLICT', message);
  }
}

export class NotFoundError extends AppError {
  constructor(resource: string, id?: string) {
    super(404, 'NOT_FOUND', id ? `${resource} ${id} not found` : `${resource} not found`);
  }
}

export class InvalidStateTransitionError extends AppError {
  constructor(
    public readonly entity: string,
    public readonly from: string,
    public readonly to: string,
  ) {
    super(409, 'INVALID_STATE_TRANSITION', `Invalid ${entity} transition: ${from} -> ${to}`);
  }
}

export class ValidationError extends AppError {
  constructor(
    message = 'Validation failed',
    public readonly details?: unknown,
  ) {
    super(400, 'VALIDATION_ERROR', message);
  }
}
