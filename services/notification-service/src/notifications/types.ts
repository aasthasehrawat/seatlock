import type { NotificationChannel } from '@seatlock/shared';

export type NotificationStatus = 'PENDING' | 'SENT' | 'FAILED';

export interface Notification {
  id: string;
  userId: string;
  channel: NotificationChannel;
  template: string;
  payload: Record<string, unknown>;
  status: NotificationStatus;
  createdAt: string;
}

export interface NewNotification {
  id: string;
  userId: string;
  channel: NotificationChannel;
  template: string;
  payload: Record<string, unknown>;
  status: NotificationStatus;
}

/** Operations that run inside one database transaction. */
export interface NotificationTx {
  /** Returns false if this event was already recorded (i.e. it is a duplicate). */
  markEventProcessed(eventId: string, eventType: string): Promise<boolean>;
  upsertHoldOwner(holdId: string, userId: string): Promise<void>;
  insertNotification(notification: NewNotification): Promise<void>;
}

/** Aggregates for the admin dashboard. */
export interface NotificationStats {
  total: number;
  byStatus: Record<NotificationStatus, number>;
  byTemplate: Record<string, number>;
  recent: Notification[];
}

export interface NotificationRepository {
  /** Counts by status/template plus the newest `recentLimit` notifications. */
  getStats(recentLimit: number): Promise<NotificationStats>;
  withTransaction<T>(fn: (tx: NotificationTx) => Promise<T>): Promise<T>;
  isEventProcessed(eventId: string): Promise<boolean>;
  findUserIdByHold(holdId: string): Promise<string | null>;
  findById(id: string): Promise<Notification | null>;
  listByUser(userId: string, limit: number): Promise<Notification[]>;
}

/** Delivery channel. The real implementation would call an email/SMS/push provider. */
export interface NotificationSender {
  send(notification: NewNotification): Promise<void>;
}
