import type pg from 'pg';
import type {
  NewNotification,
  Notification,
  NotificationRepository,
  NotificationStats,
  NotificationStatus,
  NotificationTx,
} from './types.js';
import type { NotificationChannel } from '@seatlock/shared';

interface NotificationRow {
  id: string;
  user_id: string;
  channel: NotificationChannel;
  template: string;
  payload: Record<string, unknown>;
  status: NotificationStatus;
  created_at: Date;
}

export function toNotification(row: NotificationRow): Notification {
  return {
    id: row.id,
    userId: row.user_id,
    channel: row.channel,
    template: row.template,
    payload: row.payload,
    status: row.status,
    createdAt: row.created_at.toISOString(),
  };
}

type Queryable = Pick<pg.Pool, 'query'>;

function createTx(db: Queryable): NotificationTx {
  return {
    async markEventProcessed(eventId, eventType) {
      const result = await db.query(
        `INSERT INTO processed_events (event_id, event_type)
         VALUES ($1, $2)
         ON CONFLICT (event_id) DO NOTHING`,
        [eventId, eventType],
      );
      return (result.rowCount ?? 0) > 0;
    },

    async upsertHoldOwner(holdId, userId) {
      await db.query(
        `INSERT INTO hold_owners (hold_id, user_id)
         VALUES ($1, $2)
         ON CONFLICT (hold_id) DO NOTHING`,
        [holdId, userId],
      );
    },

    async insertNotification(n: NewNotification) {
      await db.query(
        `INSERT INTO notifications (id, user_id, channel, template, payload, status)
         VALUES ($1, $2, $3, $4, $5, $6)`,
        [n.id, n.userId, n.channel, n.template, JSON.stringify(n.payload), n.status],
      );
    },
  };
}

export class PgNotificationRepository implements NotificationRepository {
  constructor(private readonly pool: pg.Pool) {}

  async withTransaction<T>(fn: (tx: NotificationTx) => Promise<T>): Promise<T> {
    const client = await this.pool.connect();
    try {
      await client.query('BEGIN');
      const result = await fn(createTx(client));
      await client.query('COMMIT');
      return result;
    } catch (err) {
      try {
        await client.query('ROLLBACK');
      } catch {
        // Connection is likely broken; the original error is more useful.
      }
      throw err;
    } finally {
      client.release();
    }
  }

  async isEventProcessed(eventId: string): Promise<boolean> {
    const { rowCount } = await this.pool.query('SELECT 1 FROM processed_events WHERE event_id = $1', [eventId]);
    return (rowCount ?? 0) > 0;
  }

  async findUserIdByHold(holdId: string): Promise<string | null> {
    const { rows } = await this.pool.query<{ user_id: string }>('SELECT user_id FROM hold_owners WHERE hold_id = $1', [
      holdId,
    ]);
    return rows[0]?.user_id ?? null;
  }

  async getStats(recentLimit: number): Promise<NotificationStats> {
    const [statusRes, templateRes, recentRes] = await Promise.all([
      this.pool.query<{ status: NotificationStatus; count: number }>(
        'SELECT status, COUNT(*)::int AS count FROM notifications GROUP BY status',
      ),
      this.pool.query<{ template: string; count: number }>(
        'SELECT template, COUNT(*)::int AS count FROM notifications GROUP BY template ORDER BY count DESC, template',
      ),
      this.pool.query<NotificationRow>('SELECT * FROM notifications ORDER BY created_at DESC, id DESC LIMIT $1', [
        recentLimit,
      ]),
    ]);

    const byStatus: Record<NotificationStatus, number> = { PENDING: 0, SENT: 0, FAILED: 0 };
    for (const r of statusRes.rows) {
      if (r.status in byStatus) byStatus[r.status] = r.count;
    }
    const byTemplate: Record<string, number> = {};
    for (const r of templateRes.rows) byTemplate[r.template] = r.count;

    return {
      total: byStatus.PENDING + byStatus.SENT + byStatus.FAILED,
      byStatus,
      byTemplate,
      recent: recentRes.rows.map(toNotification),
    };
  }

  async findById(id: string): Promise<Notification | null> {
    const { rows } = await this.pool.query<NotificationRow>('SELECT * FROM notifications WHERE id = $1', [id]);
    return rows[0] ? toNotification(rows[0]) : null;
  }

  async listByUser(userId: string, limit: number): Promise<Notification[]> {
    const { rows } = await this.pool.query<NotificationRow>(
      'SELECT * FROM notifications WHERE user_id = $1 ORDER BY created_at DESC LIMIT $2',
      [userId, limit],
    );
    return rows.map(toNotification);
  }
}
