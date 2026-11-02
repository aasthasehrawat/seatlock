import { AsyncLocalStorage } from 'node:async_hooks';
import { createId } from '@seatlock/shared';

/** HTTP header (and Kafka header name) that carries the correlation id across services. */
export const CORRELATION_HEADER = 'x-correlation-id';

const storage = new AsyncLocalStorage<{ correlationId: string }>();

const SAFE_ID = /^[A-Za-z0-9._:-]{1,128}$/;

/**
 * Accept a caller-supplied correlation id only if it looks sane; otherwise mint a new one.
 * Never trust arbitrary header content: it ends up in logs and Kafka headers.
 */
export function resolveCorrelationId(incoming?: string | string[] | null): string {
  const candidate = Array.isArray(incoming) ? incoming[0] : incoming;
  return candidate && SAFE_ID.test(candidate) ? candidate : createId();
}

/** Run `fn` with `correlationId` available to the logger via AsyncLocalStorage. */
export function runWithCorrelationId<T>(correlationId: string, fn: () => T): T {
  return storage.run({ correlationId }, fn);
}

/** The correlation id of the current async context, if any. */
export function currentCorrelationId(): string | undefined {
  return storage.getStore()?.correlationId;
}
