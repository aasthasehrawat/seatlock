import type { IHeaders } from 'kafkajs';

/**
 * Headers added when a message is parked on retry.5s or dlq. The original value and key are
 * forwarded untouched, so the retry consumer can re-run the exact same processing.
 */
export const HEADER = {
  /** Topic the message was originally consumed from. */
  ORIGINAL_TOPIC: 'x-original-topic',
  ORIGINAL_PARTITION: 'x-original-partition',
  ORIGINAL_OFFSET: 'x-original-offset',
  /** retry.5s is shared by all services; each only handles messages addressed to itself. */
  RETRY_SERVICE: 'x-retry-service',
  /** How many retries have been scheduled so far (1 on the first retry). */
  RETRY_ATTEMPT: 'x-retry-attempt',
  /** Epoch ms before which the message must not be reprocessed. */
  RETRY_AT: 'x-retry-at',
  ERROR: 'x-error',
  FAILED_SERVICE: 'x-failed-service',
  FAILED_AT: 'x-failed-at',
} as const;

/** Read a header as a string, whatever shape KafkaJS delivered it in. */
export function getHeader(headers: IHeaders | undefined, name: string): string | undefined {
  const raw = headers?.[name];
  const first = Array.isArray(raw) ? raw[0] : raw;
  return first === undefined ? undefined : first.toString();
}

/** Keep error headers small and single-line. */
export function describeError(err: unknown): string {
  const message = err instanceof Error ? `${err.name}: ${err.message}` : String(err);
  return message.replace(/\s+/g, ' ').slice(0, 500);
}
