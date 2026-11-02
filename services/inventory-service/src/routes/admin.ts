import type { FastifyPluginAsync } from 'fastify';
import { NotFoundError } from '@seatlock/shared';
import type { AppContext } from '../context.js';
import { COLUMNS, toDto, type EventRow } from './events.js';
import { parseOrThrow, uuidParam } from './helpers.js';
import { HOLD_STATUSES, queryHolds, type HoldStatus } from './holds.js';

/**
 * Admin / analytics read endpoints for the dashboard. DEMO ONLY: unauthenticated, like POST /events.
 *
 * Everything here is read-only, uncached (a live view) and built from a handful of GROUP BY
 * queries run in parallel, so one dashboard refresh costs a fixed number of round trips no matter
 * how many holds exist.
 */

const WAITLIST_STATUSES = ['WAITING', 'OFFERED', 'FULFILLED', 'CANCELLED'] as const;
type WaitlistStatus = (typeof WAITLIST_STATUSES)[number];

const RECENT_HOLDS_LIMIT = 20;

/** Zero-filled map so the UI never has to guard against missing keys. */
function zeroCounts<K extends string>(keys: readonly K[]): Record<K, number> {
  return Object.fromEntries(keys.map((k) => [k, 0])) as Record<K, number>;
}

function fillCounts<K extends string>(keys: readonly K[], rows: Array<{ status: string; count: number }>) {
  const out = zeroCounts(keys);
  for (const r of rows) {
    if ((keys as readonly string[]).includes(r.status)) out[r.status as K] = r.count;
  }
  return out;
}

interface StatusCountRow {
  status: string;
  count: number;
  tickets: number;
}

interface OverviewEventRow extends EventRow {
  held_active: number;
  waitlist_waiting: number;
}

interface HourRow {
  hour: Date;
  count: number;
  tickets: number;
}

export const adminRoutes: FastifyPluginAsync<{ ctx: AppContext }> = async (app, { ctx }) => {
  const { pool } = ctx;

  app.get('/admin/overview', async () => {
    const [totals, holdStatus, recent, waitlistStatus, events, hourly] = await Promise.all([
      pool.query<{ event_count: number; total: number; available: number }>(
        `SELECT COUNT(*)::int AS event_count,
                COALESCE(SUM(total_tickets), 0)::bigint AS total,
                COALESCE(SUM(available), 0)::bigint AS available
           FROM events`,
      ),
      pool.query<StatusCountRow>(
        `SELECT status, COUNT(*)::int AS count, COALESCE(SUM(quantity), 0)::int AS tickets
           FROM holds GROUP BY status`,
      ),
      queryHolds(pool, { limit: RECENT_HOLDS_LIMIT }),
      pool.query<StatusCountRow>(
        `SELECT status, COUNT(*)::int AS count, COALESCE(SUM(quantity), 0)::int AS tickets
           FROM waitlist GROUP BY status`,
      ),
      // Pre-aggregate per event, then join: one pass over holds/waitlist instead of a
      // correlated subquery per event row.
      pool.query<OverviewEventRow>(
        `SELECT e.id, e.name, e.description, e.venue, e.starts_at, e.total_tickets, e.available,
                e.unit_price_cents,
                COALESCE(h.held, 0)::int AS held_active,
                COALESCE(w.waiting, 0)::int AS waitlist_waiting
           FROM events e
           LEFT JOIN (SELECT event_id, SUM(quantity) AS held
                        FROM holds WHERE status = 'ACTIVE' GROUP BY event_id) h ON h.event_id = e.id
           LEFT JOIN (SELECT event_id, COUNT(*) AS waiting
                        FROM waitlist WHERE status = 'WAITING' GROUP BY event_id) w ON w.event_id = e.id
          ORDER BY e.starts_at, e.id`,
      ),
      // generate_series guarantees all 24 buckets, including empty hours, for the chart.
      pool.query<HourRow>(
        `WITH hours AS (
           SELECT generate_series(
                    date_trunc('hour', NOW()) - INTERVAL '23 hours',
                    date_trunc('hour', NOW()),
                    INTERVAL '1 hour') AS hour
         )
         SELECT hs.hour,
                COUNT(h.id)::int AS count,
                COALESCE(SUM(h.quantity), 0)::int AS tickets
           FROM hours hs
           LEFT JOIN holds h
             ON h.created_at >= hs.hour AND h.created_at < hs.hour + INTERVAL '1 hour'
          GROUP BY hs.hour
          ORDER BY hs.hour`,
      ),
    ]);

    const t = totals.rows[0];
    const totalTickets = Number(t?.total ?? 0);
    const availableTickets = Number(t?.available ?? 0);
    const reservedTickets = totalTickets - availableTickets;
    const soldPct = totalTickets > 0 ? Math.round((reservedTickets / totalTickets) * 10_000) / 100 : 0;

    const waitlistByStatus = fillCounts<WaitlistStatus>(WAITLIST_STATUSES, waitlistStatus.rows);

    return {
      generatedAt: new Date().toISOString(),
      inventory: {
        eventCount: t?.event_count ?? 0,
        totalTickets,
        availableTickets,
        reservedTickets,
        soldPct,
      },
      holds: {
        byStatus: fillCounts<HoldStatus>(HOLD_STATUSES, holdStatus.rows),
        recent,
      },
      waitlist: {
        byStatus: waitlistByStatus,
        waitingCount: waitlistByStatus.WAITING,
      },
      events: events.rows.map((r) => ({
        id: r.id,
        name: r.name,
        venue: r.venue,
        startsAt: new Date(r.starts_at).toISOString(),
        totalTickets: r.total_tickets,
        available: r.available,
        unitPriceCents: r.unit_price_cents,
        heldActive: r.held_active,
        waitlistWaiting: r.waitlist_waiting,
        // `available` already excludes both held and sold stock, so subtracting the live holds
        // leaves the confirmed-sale estimate. Clamped: the two reads are not one snapshot.
        soldEstimate: Math.max(0, r.total_tickets - r.available - r.held_active),
      })),
      activity: {
        holdsCreatedByHour: hourly.rows.map((r) => ({
          hour: new Date(r.hour).toISOString(),
          count: r.count,
          tickets: r.tickets,
        })),
      },
    };
  });

  app.get('/admin/events/:id/detail', async (request) => {
    const { id } = parseOrThrow(uuidParam, request.params, 'event id');

    const [eventRes, holdStatus, waitlistStatus, recent] = await Promise.all([
      pool.query<EventRow>(`SELECT ${COLUMNS} FROM events WHERE id = $1`, [id]),
      pool.query<StatusCountRow>(
        `SELECT status, COUNT(*)::int AS count, COALESCE(SUM(quantity), 0)::int AS tickets
           FROM holds WHERE event_id = $1 GROUP BY status`,
        [id],
      ),
      pool.query<StatusCountRow>(
        `SELECT status, COUNT(*)::int AS count, COALESCE(SUM(quantity), 0)::int AS tickets
           FROM waitlist WHERE event_id = $1 GROUP BY status`,
        [id],
      ),
      queryHolds(pool, { eventId: id, limit: RECENT_HOLDS_LIMIT }),
    ]);

    const row = eventRes.rows[0];
    if (!row) throw new NotFoundError('Event', id);

    const holdsByStatus = fillCounts<HoldStatus>(HOLD_STATUSES, holdStatus.rows);
    const waitlistByStatus = fillCounts<WaitlistStatus>(WAITLIST_STATUSES, waitlistStatus.rows);
    const activeTickets = holdStatus.rows.find((r) => r.status === 'ACTIVE')?.tickets ?? 0;
    const confirmedTickets = holdStatus.rows.find((r) => r.status === 'CONFIRMED')?.tickets ?? 0;

    return {
      generatedAt: new Date().toISOString(),
      event: toDto(row),
      holds: {
        byStatus: holdsByStatus,
        heldActive: activeTickets,
        confirmedTickets,
        recent,
      },
      waitlist: {
        byStatus: waitlistByStatus,
        waitingCount: waitlistByStatus.WAITING,
      },
    };
  });
};
