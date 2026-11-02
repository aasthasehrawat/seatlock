import type { FastifyPluginAsync } from 'fastify';
import { NotFoundError } from '@seatlock/shared';
import type { Pool } from 'pg';
import { z } from 'zod';
import type { AppContext } from '../context.js';
import { parseOrThrow, uuidParam } from './helpers.js';

export const HOLD_STATUSES = ['ACTIVE', 'CONFIRMED', 'EXPIRED', 'RELEASED'] as const;
export type HoldStatus = (typeof HOLD_STATUSES)[number];

const ListHoldsQuery = z.object({
  eventId: z.string().uuid().optional(),
  status: z.enum(HOLD_STATUSES).optional(),
  limit: z.coerce.number().int().min(1).max(200).default(50),
});

interface HoldListRow {
  id: string;
  event_id: string;
  event_name: string;
  user_id: string;
  quantity: number;
  status: HoldStatus;
  expires_at: Date;
  order_id: string | null;
  created_at: Date;
}

export interface HoldSummary {
  id: string;
  eventId: string;
  eventName: string;
  userId: string;
  quantity: number;
  status: HoldStatus;
  expiresAt: string;
  createdAt: string;
  orderId: string | null;
}

export interface HoldListFilter {
  eventId?: string;
  status?: HoldStatus;
  limit: number;
}

/** Newest-first hold listing joined with event names. Shared by GET /holds and the admin routes. */
export async function queryHolds(pool: Pick<Pool, 'query'>, filter: HoldListFilter): Promise<HoldSummary[]> {
  const { rows } = await pool.query<HoldListRow>(
    `SELECT h.id, h.event_id, e.name AS event_name, h.user_id, h.quantity, h.status,
            h.expires_at, h.order_id, h.created_at
       FROM holds h
       JOIN events e ON e.id = h.event_id
      WHERE ($1::uuid IS NULL OR h.event_id = $1::uuid)
        AND ($2::text IS NULL OR h.status = $2::text)
      ORDER BY h.created_at DESC, h.id DESC
      LIMIT $3`,
    [filter.eventId ?? null, filter.status ?? null, filter.limit],
  );
  return rows.map((r) => ({
    id: r.id,
    eventId: r.event_id,
    eventName: r.event_name,
    userId: r.user_id,
    quantity: r.quantity,
    status: r.status,
    expiresAt: new Date(r.expires_at).toISOString(),
    createdAt: new Date(r.created_at).toISOString(),
    orderId: r.order_id,
  }));
}

interface HoldRow {
  id: string;
  event_id: string;
  user_id: string;
  quantity: number;
  status: string;
  expires_at: Date;
  order_id: string | null;
  seconds_remaining: number;
}

export const holdRoutes: FastifyPluginAsync<{ ctx: AppContext }> = async (app, { ctx }) => {
  /**
   * GET /holds?eventId=&status=&limit=: admin table, newest first. Not cached (live view).
   * Registered alongside /holds/:id; find-my-way keeps the static and parametric routes apart.
   */
  app.get('/holds', async (request) => {
    const filter = parseOrThrow(ListHoldsQuery, request.query, 'hold query');
    const holds = await queryHolds(ctx.pool, filter);
    return { holds, count: holds.length, limit: filter.limit };
  });
  /**
   * GET /holds/:id. Not cached: clients poll this to drive their checkout countdown, and it must
   * reflect the authoritative state (e.g. EXPIRED) immediately.
   * `secondsRemaining` is computed with the DB clock so it agrees with the expiry sweeper.
   */
  app.get('/holds/:id', async (request) => {
    const { id } = parseOrThrow(uuidParam, request.params, 'hold id');
    const { rows } = await ctx.pool.query<HoldRow>(
      `SELECT id, event_id, user_id, quantity, status, expires_at, order_id,
              CASE WHEN status = 'ACTIVE'
                   THEN GREATEST(0, CEIL(EXTRACT(EPOCH FROM (expires_at - NOW()))))::int
                   ELSE 0 END AS seconds_remaining
         FROM holds
        WHERE id = $1`,
      [id],
    );
    const row = rows[0];
    if (!row) throw new NotFoundError('Hold', id);

    return {
      holdId: row.id,
      eventId: row.event_id,
      userId: row.user_id,
      quantity: row.quantity,
      status: row.status,
      expiresAt: new Date(row.expires_at).toISOString(),
      secondsRemaining: row.seconds_remaining,
      orderId: row.order_id,
    };
  });
};
