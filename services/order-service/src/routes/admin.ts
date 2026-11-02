import type { FastifyPluginAsync } from 'fastify';
import { AppError } from '@seatlock/shared';
import type { OrderAnalytics } from '../db/analytics-repo.js';
import { toOrderDto } from './orders.js';

const RECENT_ORDERS_LIMIT = 20;

export interface AdminRoutesOptions {
  /** Absent in tests that don't need analytics; the route then answers 503 instead of crashing. */
  analytics?: OrderAnalytics;
}

/**
 * GET /admin/overview: one payload for the admin dashboard (orders by status, revenue, recent
 * orders, 24h hourly activity). DEMO ONLY: unauthenticated, like the inventory admin endpoints.
 */
export const adminRoutes: FastifyPluginAsync<AdminRoutesOptions> = async (app, { analytics }) => {
  app.get('/admin/overview', async () => {
    if (!analytics) {
      throw new AppError(503, 'ADMIN_UNAVAILABLE', 'Order analytics are not configured');
    }
    const data = await analytics.getOverview(RECENT_ORDERS_LIMIT);

    return {
      generatedAt: new Date().toISOString(),
      orders: {
        byStatus: data.byStatus,
        totalRevenueCents: data.totalRevenueCents,
        recent: data.recent.map(toOrderDto),
      },
      activity: {
        ordersByHour: data.hourly.map((b) => ({
          hour: b.hour.toISOString(),
          count: b.count,
          confirmed: b.confirmed,
          failed: b.failed,
          revenueCents: b.revenueCents,
        })),
      },
    };
  });
};
