import { createHash } from 'node:crypto';

/** Recursively sort object keys so logically equal values serialise identically. */
function canonicalize(value: unknown): unknown {
  if (Array.isArray(value)) return value.map(canonicalize);
  if (value !== null && typeof value === 'object') {
    return Object.fromEntries(
      Object.entries(value as Record<string, unknown>)
        .sort(([a], [b]) => (a < b ? -1 : a > b ? 1 : 0))
        .map(([k, v]) => [k, canonicalize(v)]),
    );
  }
  return value;
}

/**
 * Stable SHA-256 fingerprint of a request. Stored next to an idempotency key so we can tell
 * a legitimate retry (same hash) from key reuse with a different request (different hash).
 */
export function hashRequest(value: unknown): string {
  return createHash('sha256').update(JSON.stringify(canonicalize(value))).digest('hex');
}
