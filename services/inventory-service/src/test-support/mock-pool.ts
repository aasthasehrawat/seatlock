import type { PoolClient } from 'pg';
import { vi } from 'vitest';
import type { TxPool } from '../db/pool.js';

export interface QueryCall {
  /** SQL with whitespace collapsed, for easy `includes` matching. */
  sql: string;
  values: unknown[];
}

export interface FakeResult {
  rows?: unknown[];
  rowCount?: number;
}

/** Return a result to answer the query, or undefined to fall through to the empty default. */
export type Responder = (sql: string, values: unknown[]) => FakeResult | undefined;

/**
 * Minimal pg Pool/Client double. It records every statement so tests can assert on exactly what
 * ran (and in what order), and lets each test script the database's answers by SQL substring.
 */
export function createMockPool(responder: Responder) {
  const calls: QueryCall[] = [];

  const client = {
    query: vi.fn(async (text: string, values: unknown[] = []) => {
      const sql = text.replace(/\s+/g, ' ').trim();
      calls.push({ sql, values });
      const result = responder(sql, values) ?? {};
      const rows = result.rows ?? [];
      return { rows, rowCount: result.rowCount ?? rows.length, command: '', oid: 0, fields: [] };
    }),
    release: vi.fn(),
  };

  const pool: TxPool = { connect: vi.fn(async () => client as unknown as PoolClient) };

  return {
    pool,
    client,
    calls,
    /** All statements whose SQL contains `fragment`. */
    find: (fragment: string) => calls.filter((c) => c.sql.includes(fragment)),
    has: (fragment: string) => calls.some((c) => c.sql.includes(fragment)),
    /** Parsed envelope payloads inserted into the outbox, in order. */
    outboxEvents: () =>
      calls
        .filter((c) => c.sql.startsWith('INSERT INTO outbox'))
        .map((c) => JSON.parse(c.values[4] as string) as { type: string; payload: Record<string, unknown> }),
  };
}
