import type pg from 'pg';
import type {
  NewPayment,
  OutboxMessage,
  Payment,
  PaymentRepository,
  PaymentResult,
  PaymentStatus,
  PaymentTx,
} from './types.js';

interface PaymentRow {
  id: string;
  order_id: string;
  hold_id: string;
  user_id: string;
  amount_cents: number;
  currency: string;
  status: PaymentStatus;
  provider_ref: string | null;
  failure_reason: string | null;
  attempt_count: number;
  created_at: Date;
  updated_at: Date;
}

export function toPayment(row: PaymentRow): Payment {
  return {
    id: row.id,
    orderId: row.order_id,
    holdId: row.hold_id,
    userId: row.user_id,
    amountCents: row.amount_cents,
    currency: row.currency,
    status: row.status,
    providerRef: row.provider_ref,
    failureReason: row.failure_reason,
    attemptCount: row.attempt_count,
    createdAt: row.created_at.toISOString(),
    updatedAt: row.updated_at.toISOString(),
  };
}

type Queryable = Pick<pg.Pool, 'query'>;

function createTx(db: Queryable): PaymentTx {
  return {
    async lockById(id) {
      const { rows } = await db.query<PaymentRow>('SELECT * FROM payments WHERE id = $1 FOR UPDATE', [id]);
      return rows[0] ? toPayment(rows[0]) : null;
    },

    async findByOrderId(orderId) {
      const { rows } = await db.query<PaymentRow>('SELECT * FROM payments WHERE order_id = $1', [orderId]);
      return rows[0] ? toPayment(rows[0]) : null;
    },

    async insertPendingIfAbsent(payment: NewPayment) {
      // No conflict target: covers both UNIQUE(order_id) and the primary key.
      await db.query(
        `INSERT INTO payments (id, order_id, hold_id, user_id, amount_cents, currency, status)
         VALUES ($1, $2, $3, $4, $5, $6, 'PENDING')
         ON CONFLICT DO NOTHING`,
        [payment.id, payment.orderId, payment.holdId, payment.userId, payment.amountCents, payment.currency],
      );
    },

    async recordResult(id: string, result: PaymentResult) {
      const { rows } = await db.query<PaymentRow>(
        `UPDATE payments
            SET status = $2,
                provider_ref = $3,
                failure_reason = $4,
                attempt_count = attempt_count + 1,
                updated_at = NOW()
          WHERE id = $1
          RETURNING *`,
        [id, result.status, result.providerRef, result.failureReason],
      );
      if (!rows[0]) {
        throw new Error(`Payment ${id} disappeared while recording result`);
      }
      return toPayment(rows[0]);
    },

    async enqueueOutbox(message: OutboxMessage) {
      await db.query(
        `INSERT INTO outbox (event_id, topic, message_key, event_type, payload)
         VALUES ($1, $2, $3, $4, $5)
         ON CONFLICT (event_id) DO NOTHING`,
        [message.eventId, message.topic, message.key, message.envelope.type, JSON.stringify(message.envelope)],
      );
    },

    async markEventProcessed(eventId: string, eventType: string) {
      const result = await db.query(
        `INSERT INTO processed_events (event_id, event_type)
         VALUES ($1, $2)
         ON CONFLICT (event_id) DO NOTHING`,
        [eventId, eventType],
      );
      return (result.rowCount ?? 0) > 0;
    },
  };
}

export class PgPaymentRepository implements PaymentRepository {
  constructor(private readonly pool: pg.Pool) {}

  async withTransaction<T>(fn: (tx: PaymentTx) => Promise<T>): Promise<T> {
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

  async findById(id: string): Promise<Payment | null> {
    const { rows } = await this.pool.query<PaymentRow>('SELECT * FROM payments WHERE id = $1', [id]);
    return rows[0] ? toPayment(rows[0]) : null;
  }

  async findByOrderId(orderId: string): Promise<Payment | null> {
    const { rows } = await this.pool.query<PaymentRow>('SELECT * FROM payments WHERE order_id = $1', [orderId]);
    return rows[0] ? toPayment(rows[0]) : null;
  }

  async list(filter: { orderId?: string; limit: number }): Promise<Payment[]> {
    const { rows } = await this.pool.query<PaymentRow>(
      `SELECT * FROM payments
        WHERE ($1::uuid IS NULL OR order_id = $1::uuid)
        ORDER BY created_at DESC
        LIMIT $2`,
      [filter.orderId ?? null, filter.limit],
    );
    return rows.map(toPayment);
  }
}
