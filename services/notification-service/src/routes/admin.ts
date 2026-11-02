import type { FastifyPluginAsync } from 'fastify';
import type { NotificationRepository } from '../notifications/types.js';
import { toNotificationDto } from './notifications.js';

export interface AdminRouteOptions {
  repo: NotificationRepository;
}

const RECENT_LIMIT = 15;

/**
 * GET /admin/overview: delivery totals, status/template breakdowns and the newest notifications.
 * DEMO ONLY: unauthenticated, like the other admin endpoints.
 */
export const adminRoutes: FastifyPluginAsync<AdminRouteOptions> = async (app, { repo }) => {
  app.get('/admin/overview', async () => {
    const stats = await repo.getStats(RECENT_LIMIT);
    return {
      generatedAt: new Date().toISOString(),
      total: stats.total,
      byStatus: stats.byStatus,
      byTemplate: stats.byTemplate,
      recent: stats.recent.map(toNotificationDto),
    };
  });
};
