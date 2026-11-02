import { v4 as uuidv4 } from 'uuid';
import { ValidationError } from './errors.js';

/** New random UUID v4. */
export function createId(): string {
  return uuidv4();
}

/** Current time as an ISO-8601 UTC string. */
export function nowIso(): string {
  return new Date().toISOString();
}

/**
 * Money is always stored as integer cents to avoid floating-point errors.
 * Format cents for display, e.g. 12345 -> "$123.45".
 */
export function formatCents(cents: number, currency = 'USD'): string {
  if (!Number.isInteger(cents)) {
    throw new ValidationError(`Cents must be an integer, got ${cents}`);
  }
  return new Intl.NumberFormat('en-US', { style: 'currency', currency }).format(cents / 100);
}

/**
 * Parse a decimal money string into integer cents, e.g. "$1,234.50" -> 123450.
 * Uses string parsing only (no floats) so "19.99" never becomes 1998.9999.
 */
export function parseCents(input: string): number {
  const cleaned = input.trim().replace(/^\$/, '').replace(/,/g, '');
  const match = /^(-?)(\d+)(?:\.(\d{1,2}))?$/.exec(cleaned);
  if (!match) {
    throw new ValidationError(`Invalid money amount: "${input}"`);
  }
  const [, sign, whole, fraction = ''] = match;
  const cents = Number(whole) * 100 + Number(fraction.padEnd(2, '0'));
  return sign === '-' ? -cents : cents;
}
