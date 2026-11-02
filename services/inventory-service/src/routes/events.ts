import type { FastifyPluginAsync } from 'fastify';
import { NotFoundError, createId } from '@seatlock/shared';
import { z } from 'zod';
import type { AppContext } from '../context.js';
import { getOrSetJson } from '../lib/redis.js';
import { parseOrThrow, uuidParam } from './helpers.js';

export interface EventRow {
  id: string;
  name: string;
  description: string | null;
  venue: string | null;
  starts_at: Date;
  total_tickets: number;
  available: number;
  unit_price_cents: number;
}

export const COLUMNS =
  'id, name, description, venue, starts_at, total_tickets, available, unit_price_cents';

/** Postgres INT (int4) ceiling: unit_price_cents is stored in an INT column. */
const MAX_INT4 = 2_147_483_647;

/**
 * POST /events body. DEMO ONLY: the endpoint is unauthenticated so the admin UI can create
 * events without a login flow. A real deployment must put this behind authN/authZ.
 */
const CreateEventSchema = z.object({
  name: z.string().trim().min(1).max(200),
  description: z.string().trim().max(5_000).optional(),
  venue: z.string().trim().max(500).optional(),
  startsAt: z.string().datetime({ offset: true }),
  totalTickets: z.number().int().positive().max(100_000),
  unitPriceCents: z.number().int().nonnegative().max(MAX_INT4),
});

export function toDto(row: EventRow) {
  return {
    id: row.id,
    name: row.name,
    description: row.description,
    venue: row.venue,
    startsAt: new Date(row.starts_at).toISOString(),
    totalTickets: row.total_tickets,
    available: row.available,
    unitPriceCents: row.unit_price_cents,
  };
}

/**
 * Read endpoints, cached in Redis for a few seconds.
 *
 * Why only a short TTL and no invalidation on every sale? During a flash sale, `available` changes
 * on every reservation; invalidating on each write would make the cache useless exactly when the
 * read load peaks. A 5s TTL caps database reads at ~1 per key per 5s while keeping the displayed
 * stock close enough. It is advisory only: the authoritative check is the atomic UPDATE in
 * POST /reservations, so a stale "3 left" can at worst lead to a clean 409 SOLD_OUT.
 */
export const eventRoutes: FastifyPluginAsync<{ ctx: AppContext }> = async (app, { ctx }) => {
  const ttl = ctx.config.cacheTtlSeconds;

  app.get('/events', async (_request, reply) => {
    const { value, hit } = await getOrSetJson(ctx.redis, 'inventory:events:list', ttl, async () => {
      const { rows } = await ctx.pool.query<EventRow>(
        `SELECT ${COLUMNS} FROM events ORDER BY starts_at, id`,
      );
      return { events: rows.map(toDto) };
    });
    return reply.header('X-Cache', hit ? 'HIT' : 'MISS').send(value);
  });

  /**
   * POST /events: create an event. DEMO ONLY, no authentication (see CreateEventSchema).
   * Stock starts fully available. Only the list cache is dropped: the per-event key can't exist
   * yet for a brand-new id.
   */
  app.post('/events', async (request, reply) => {
    const body = parseOrThrow(CreateEventSchema, request.body, 'event');
    const { rows } = await ctx.pool.query<EventRow>(
      `INSERT INTO events (id, name, description, venue, starts_at, total_tickets, available, unit_price_cents)
       VALUES ($1, $2, $3, $4, $5, $6, $6, $7)
       RETURNING ${COLUMNS}`,
      [
        createId(),
        body.name,
        body.description ?? null,
        body.venue ?? null,
        body.startsAt,
        body.totalTickets,
        body.unitPriceCents,
      ],
    );
    const row = rows[0];
    if (!row) throw new Error('event insert returned no row');

    if (ctx.redis) {
      try {
        await ctx.redis.del('inventory:events:list');
      } catch (err) {
        // Cache is advisory; it self-heals when the short TTL elapses.
        request.log.warn({ err: (err as Error).message }, 'failed to invalidate events list cache');
      }
    }

    return reply.code(201).send(toDto(row));
  });

  app.get('/events/:id', async (request, reply) => {
    const { id } = parseOrThrow(uuidParam, request.params, 'event id');
    const { value, hit } = await getOrSetJson(ctx.redis, `inventory:events:${id}`, ttl, async () => {
      const { rows } = await ctx.pool.query<EventRow>(`SELECT ${COLUMNS} FROM events WHERE id = $1`, [id]);
      const row = rows[0];
      // Throwing here means "not found" is never cached.
      if (!row) throw new NotFoundError('Event', id);
      return toDto(row);
    });
    return reply.header('X-Cache', hit ? 'HIT' : 'MISS').send(value);
  });
};
