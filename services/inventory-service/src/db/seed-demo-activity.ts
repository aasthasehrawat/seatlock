import '../lib/load-env.js';
import { pathToFileURL } from 'node:url';
import type { Pool } from 'pg';
import { createId } from '@seatlock/shared';
import { closePool, getPool, withTransaction } from './pool.js';
import { logger } from '../lib/logger.js';

/**
 * Demo data for the admin analytics charts.
 *
 * Creates ONE extra event ("Demo Analytics Concert", 500 tickets) and ~30 historical holds spread
 * across the last 24 hours in terminal states only (CONFIRMED / EXPIRED / RELEASED).
 *
 * Why this cannot corrupt inventory math:
 *  - Only the demo event is touched; real events and their stock are never modified.
 *  - No ACTIVE holds are created, so the expiry sweeper has nothing to act on and stock is never
 *    "stuck" behind a hold that will release later.
 *  - EXPIRED / RELEASED holds return their tickets, CONFIRMED holds keep them, so
 *      available = totalTickets - SUM(quantity of CONFIRMED holds)
 *    is exactly what the real reservation flow would have produced.
 *  - Idempotent: re-running deletes the demo event's previous holds/waitlist rows and rebuilds
 *    them, recomputing `available` from scratch instead of decrementing again.
 *  - No outbox rows are written, so nothing is published to Kafka.
 */
export const DEMO_EVENT = {
  id: '00000000-0000-4000-8000-00000000d001',
  name: 'Demo Analytics Concert',
  description: 'Synthetic event with historical holds, used to populate admin dashboard charts.',
  venue: 'Demo Arena, Anywhere',
  totalTickets: 500,
  unitPriceCents: 6_500,
} as const;

type TerminalStatus = 'CONFIRMED' | 'EXPIRED' | 'RELEASED';

/** Fixed mix: 14 confirmed, 10 expired, 6 released = 30 holds. */
const STATUS_PLAN: readonly TerminalStatus[] = [
  ...Array<TerminalStatus>(14).fill('CONFIRMED'),
  ...Array<TerminalStatus>(10).fill('EXPIRED'),
  ...Array<TerminalStatus>(6).fill('RELEASED'),
];

const WAITLIST_PLAN = ['WAITING', 'WAITING', 'WAITING', 'OFFERED', 'FULFILLED', 'CANCELLED'] as const;

const HOLD_TTL_MINUTES = 10;

/** Small deterministic PRNG (mulberry32) so repeated runs produce the same shape of data. */
function prng(seed: number): () => number {
  let a = seed >>> 0;
  return () => {
    a = (a + 0x6d2b79f5) >>> 0;
    let t = a;
    t = Math.imul(t ^ (t >>> 15), t | 1);
    t ^= t + Math.imul(t ^ (t >>> 7), t | 61);
    return ((t ^ (t >>> 14)) >>> 0) / 4_294_967_296;
  };
}

export interface DemoSeedResult {
  eventId: string;
  holds: number;
  confirmedTickets: number;
  available: number;
}

export async function seedDemoActivity(pool: Pick<Pool, 'connect'> = getPool()): Promise<DemoSeedResult> {
  const rand = prng(20_261_102);
  const now = Date.now();
  const minute = 60_000;

  return withTransaction(pool, async (client) => {
    await client.query(
      `INSERT INTO events (id, name, description, venue, starts_at, total_tickets, available, unit_price_cents)
       VALUES ($1, $2, $3, $4, NOW() + INTERVAL '60 days', $5, $5, $6)
       ON CONFLICT (id) DO UPDATE SET
         name = EXCLUDED.name,
         description = EXCLUDED.description,
         venue = EXCLUDED.venue,
         total_tickets = EXCLUDED.total_tickets,
         unit_price_cents = EXCLUDED.unit_price_cents`,
      [
        DEMO_EVENT.id,
        DEMO_EVENT.name,
        DEMO_EVENT.description,
        DEMO_EVENT.venue,
        DEMO_EVENT.totalTickets,
        DEMO_EVENT.unitPriceCents,
      ],
    );

    // Rebuild from scratch (idempotent). Scoped strictly to the demo event.
    await client.query('DELETE FROM holds WHERE event_id = $1', [DEMO_EVENT.id]);
    await client.query('DELETE FROM waitlist WHERE event_id = $1', [DEMO_EVENT.id]);

    let confirmedTickets = 0;
    for (const status of STATUS_PLAN) {
      const quantity = 1 + Math.floor(rand() * 4); // 1..4
      // Skew towards recent hours so the chart looks like a ramping sale (rand^1.6).
      const ageMinutes = Math.floor(Math.pow(rand(), 1.6) * 23 * 60) + 2;
      const createdAt = new Date(now - ageMinutes * minute);
      const expiresAt = new Date(createdAt.getTime() + HOLD_TTL_MINUTES * minute);
      const updatedAt = new Date(
        status === 'RELEASED'
          ? createdAt.getTime() + Math.floor(rand() * 8 + 1) * minute
          : expiresAt.getTime() + (status === 'CONFIRMED' ? -2 * minute : 30_000),
      );
      if (status === 'CONFIRMED') confirmedTickets += quantity;

      await client.query(
        `INSERT INTO holds (id, event_id, user_id, quantity, status, expires_at, order_id, created_at, updated_at)
         VALUES ($1, $2, $3, $4, $5, $6, NULL, $7, $8)`,
        [createId(), DEMO_EVENT.id, createId(), quantity, status, expiresAt, createdAt, updatedAt],
      );
    }

    for (const [i, status] of WAITLIST_PLAN.entries()) {
      await client.query(
        `INSERT INTO waitlist (id, event_id, user_id, quantity, status, priority, offered_hold_id, created_at)
         VALUES ($1, $2, $3, $4, $5, 0, NULL, $6)`,
        [
          createId(),
          DEMO_EVENT.id,
          createId(),
          1 + (i % 3),
          status,
          new Date(now - (30 + i * 45) * minute),
        ],
      );
    }

    // Recompute stock from the holds that really consumed it (see header comment).
    const available = DEMO_EVENT.totalTickets - confirmedTickets;
    await client.query('UPDATE events SET available = $2 WHERE id = $1', [DEMO_EVENT.id, available]);

    return { eventId: DEMO_EVENT.id, holds: STATUS_PLAN.length, confirmedTickets, available };
  });
}

const isMain = process.argv[1] !== undefined && import.meta.url === pathToFileURL(process.argv[1]).href;

if (isMain) {
  seedDemoActivity()
    .then((result) => {
      logger.info(result, 'demo analytics seed complete');
    })
    .catch((err) => {
      logger.error({ err }, 'demo analytics seed failed');
      process.exitCode = 1;
    })
    .finally(() => closePool());
}
