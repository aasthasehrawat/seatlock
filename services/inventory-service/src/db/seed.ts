import '../lib/load-env.js';
import { pathToFileURL } from 'node:url';
import type { Pool } from 'pg';
import { closePool, getPool, withTransaction } from './pool.js';
import { logger } from '../lib/logger.js';

/**
 * Fixed UUIDs so load tests, docs and the frontend can reference concerts
 * without first calling GET /events.
 */
export const SEED_EVENTS = [
  {
    id: '00000000-0000-4000-8000-00000000a001',
    name: 'Neon Horizon World Tour',
    description: 'Synth-pop headliner. Flash sale, strictly limited tickets.',
    venue: 'Madison Square Garden, New York',
    startsAt: '2027-03-14T19:30:00Z',
    totalTickets: 500,
    unitPriceCents: 12_500,
  },
  {
    // Deliberately tiny: ideal for load tests that must demonstrate "no overselling".
    id: '00000000-0000-4000-8000-00000000a002',
    name: 'Midnight Echoes - Load Test Show',
    description: 'Only 100 tickets. Hammer it with concurrent requests: exactly 100 should succeed.',
    venue: 'The Fillmore, San Francisco',
    startsAt: '2027-04-02T20:00:00Z',
    totalTickets: 100,
    unitPriceCents: 8_000,
  },
  {
    id: '00000000-0000-4000-8000-00000000a003',
    name: 'Acoustic Sundown Sessions',
    description: 'Intimate unplugged set.',
    venue: 'Red Rocks Amphitheatre, Colorado',
    startsAt: '2027-05-21T18:00:00Z',
    totalTickets: 40,
    unitPriceCents: 4_500,
  },
] as const;

export interface SeedOptions {
  /** Wipe all transactional state and restore full stock. Useful before each load test. */
  reset?: boolean;
}

export async function seed(pool: Pick<Pool, 'connect'> = getPool(), opts: SeedOptions = {}): Promise<void> {
  await withTransaction(pool, async (client) => {
    if (opts.reset) {
      await client.query(
        'TRUNCATE TABLE outbox, processed_events, idempotency_keys, waitlist, holds RESTART IDENTITY',
      );
      // All holds are gone, so every event (incl. ones created via POST /events or seed:demo)
      // gets its full stock back; otherwise their `available` would be permanently understated.
      await client.query('UPDATE events SET available = total_tickets');
    }
    for (const e of SEED_EVENTS) {
      await client.query(
        `INSERT INTO events (id, name, description, venue, starts_at, total_tickets, available, unit_price_cents)
         VALUES ($1, $2, $3, $4, $5, $6, $6, $7)
         ON CONFLICT (id) DO UPDATE SET
           name = EXCLUDED.name,
           description = EXCLUDED.description,
           venue = EXCLUDED.venue,
           starts_at = EXCLUDED.starts_at,
           unit_price_cents = EXCLUDED.unit_price_cents,
           total_tickets = CASE WHEN $8::boolean THEN EXCLUDED.total_tickets ELSE events.total_tickets END,
           available = CASE WHEN $8::boolean THEN EXCLUDED.available ELSE events.available END`,
        [e.id, e.name, e.description, e.venue, e.startsAt, e.totalTickets, e.unitPriceCents, opts.reset === true],
      );
    }
  });
}

const isMain = process.argv[1] !== undefined && import.meta.url === pathToFileURL(process.argv[1]).href;

if (isMain) {
  const reset = process.argv.includes('--reset');
  seed(getPool(), { reset })
    .then(() => {
      logger.info({ reset, events: SEED_EVENTS.map((e) => ({ id: e.id, name: e.name })) }, 'seed complete');
    })
    .catch((err) => {
      logger.error({ err }, 'seed failed');
      process.exitCode = 1;
    })
    .finally(() => closePool());
}
