import type { FastifyPluginAsync } from 'fastify';
import { NotFoundError } from '@seatlock/shared';
import { z } from 'zod';
import type { Notification, NotificationRepository } from '../notifications/types.js';

export interface NotificationRouteOptions {
  repo: NotificationRepository;
}

const IdParams = z.object({ id: z.string().uuid() });

const ListQuery = z.object({
  userId: z.string().uuid(),
  limit: z.coerce.number().int().min(1).max(200).default(50),
});

/** Public JSON shape of a notification. */
export function toNotificationDto(notification: Notification) {
  return {
    id: notification.id,
    userId: notification.userId,
    channel: notification.channel,
    template: notification.template,
    payload: notification.payload,
    status: notification.status,
    createdAt: notification.createdAt,
  };
}

export const notificationRoutes: FastifyPluginAsync<NotificationRouteOptions> = async (app, opts) => {
  app.get('/notifications', async (request) => {
    const query = ListQuery.parse(request.query);
    const notifications = await opts.repo.listByUser(query.userId, query.limit);
    return { notifications: notifications.map(toNotificationDto) };
  });

  app.get('/notifications/:id', async (request) => {
    const { id } = IdParams.parse(request.params);
    const notification = await opts.repo.findById(id);
    if (!notification) throw new NotFoundError('Notification', id);
    return toNotificationDto(notification);
  });
};
