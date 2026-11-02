import { ORDER_STATES, type OrderState } from '@seatlock/shared';
import type { Order, Queryable } from './types.js';

export interface OrderHourBucket {
  hour: Date;
  count: number;
  confirmed: number;
  failed: number;
  revenueCents: number;
}

export interface OrderOverviewData {
  byStatus: Record<OrderState, number>;
  totalRevenueCents: number;
  recent: Order[];
  hourly: OrderHourBucket[];
}

/** Read-only dashboard queries. Kept apart from OrdersRepo: it is not part of any transaction. */
export interface OrderAnalytics {
  getOverview(recentLimit: number): Promise<OrderOverviewData>;
}

interface OrderRow {
  id: string;
  hold_id: string;
  event_id: string;
  user_id: string;
  quantity: number;
  amount_cents: number;
  status: OrderState;
  payment_id: string | null;
  created_at: Date;
  updated_at: Date;
}

export class PgOrderAnalytics implements OrderAnalytics {
  constructor(private readonly db: Queryable) {}

  async getOverview(recentLimit: number): Promise<OrderOverviewData> {
    const [statusRes, revenueRes, recentRes, hourlyRes] = await Promise.all([
      this.db.query<{ status: OrderState; count: number }>(
        'SELECT status, COUNT(*)::int AS count FROM orders GROUP BY status',
      ),
      this.db.query<{ revenue: string }>(
        `SELECT COALESCE(SUM(amount_cents), 0)::bigint AS revenue FROM orders WHERE status = 'CONFIRMED'`,
      ),
      this.db.query<OrderRow>(
        `SELECT id, hold_id, event_id, user_id, quantity, amount_cents, status, payment_id,
                created_at, updated_at
           FROM orders
          ORDER BY created_at DESC, id DESC
          LIMIT $1`,
        [recentLimit],
      ),
      // generate_series yields all 24 buckets, including hours with no orders, for charting.
      // Orders are bucketed by creation hour; `confirmed`/`failed` are their current status.
      this.db.query<{ hour: Date; count: number; confirmed: number; failed: number; revenue: string }>(
        `WITH hours AS (
           SELECT generate_series(
                    date_trunc('hour', NOW()) - INTERVAL '23 hours',
                    date_trunc('hour', NOW()),
                    INTERVAL '1 hour') AS hour
         )
         SELECT hs.hour,
                COUNT(o.id)::int AS count,
                COUNT(o.id) FILTER (WHERE o.status = 'CONFIRMED')::int AS confirmed,
                COUNT(o.id) FILTER (WHERE o.status = 'FAILED')::int AS failed,
                COALESCE(SUM(o.amount_cents) FILTER (WHERE o.status = 'CONFIRMED'), 0)::bigint AS revenue
           FROM hours hs
           LEFT JOIN orders o
             ON o.created_at >= hs.hour AND o.created_at < hs.hour + INTERVAL '1 hour'
          GROUP BY hs.hour
          ORDER BY hs.hour`,
      ),
    ]);

    const byStatus = Object.fromEntries(Object.values(ORDER_STATES).map((s) => [s, 0])) as Record<
      OrderState,
      number
    >;
    for (const r of statusRes.rows) {
      if (r.status in byStatus) byStatus[r.status] = r.count;
    }

    return {
      byStatus,
      totalRevenueCents: Number(revenueRes.rows[0]?.revenue ?? 0),
      recent: recentRes.rows.map((row) => ({
        id: row.id,
        holdId: row.hold_id,
        eventId: row.event_id,
        userId: row.user_id,
        quantity: row.quantity,
        amountCents: row.amount_cents,
        status: row.status,
        paymentId: row.payment_id,
        createdAt: row.created_at,
        updatedAt: row.updated_at,
      })),
      hourly: hourlyRes.rows.map((r) => ({
        hour: r.hour,
        count: r.count,
        confirmed: r.confirmed,
        failed: r.failed,
        revenueCents: Number(r.revenue),
      })),
    };
  }
}
