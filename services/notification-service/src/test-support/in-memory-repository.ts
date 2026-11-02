import type {
  NewNotification,
  Notification,
  NotificationRepository,
  NotificationStats,
  NotificationStatus,
  NotificationTx,
} from '../notifications/types.js';

/**
 * In-memory NotificationRepository for unit tests.
 * Transactions are serialised through a promise queue; there is no rollback.
 */
export class InMemoryNotificationRepository implements NotificationRepository {
  readonly notifications: Notification[] = [];
  readonly processedEvents = new Set<string>();
  readonly holdOwners = new Map<string, string>();

  private queue: Promise<unknown> = Promise.resolve();
  private tick = 0;

  private readonly tx: NotificationTx = {
    markEventProcessed: async (eventId) => {
      if (this.processedEvents.has(eventId)) return false;
      this.processedEvents.add(eventId);
      return true;
    },

    upsertHoldOwner: async (holdId, userId) => {
      if (!this.holdOwners.has(holdId)) this.holdOwners.set(holdId, userId);
    },

    insertNotification: async (n: NewNotification) => {
      this.tick += 1;
      this.notifications.push({
        ...n,
        createdAt: new Date(Date.UTC(2026, 0, 1, 0, 0, this.tick)).toISOString(),
      });
    },
  };

  withTransaction<T>(fn: (tx: NotificationTx) => Promise<T>): Promise<T> {
    const run = this.queue.then(() => fn(this.tx));
    this.queue = run.catch(() => undefined);
    return run;
  }

  async isEventProcessed(eventId: string): Promise<boolean> {
    return this.processedEvents.has(eventId);
  }

  async findUserIdByHold(holdId: string): Promise<string | null> {
    return this.holdOwners.get(holdId) ?? null;
  }

  async getStats(recentLimit: number): Promise<NotificationStats> {
    const byStatus: Record<NotificationStatus, number> = { PENDING: 0, SENT: 0, FAILED: 0 };
    const byTemplate: Record<string, number> = {};
    for (const n of this.notifications) {
      byStatus[n.status] += 1;
      byTemplate[n.template] = (byTemplate[n.template] ?? 0) + 1;
    }
    return {
      total: this.notifications.length,
      byStatus,
      byTemplate,
      recent: [...this.notifications].sort((a, b) => b.createdAt.localeCompare(a.createdAt)).slice(0, recentLimit),
    };
  }

  async findById(id: string): Promise<Notification | null> {
    return this.notifications.find((n) => n.id === id) ?? null;
  }

  async listByUser(userId: string, limit: number): Promise<Notification[]> {
    return this.notifications
      .filter((n) => n.userId === userId)
      .sort((a, b) => b.createdAt.localeCompare(a.createdAt))
      .slice(0, limit);
  }
}
