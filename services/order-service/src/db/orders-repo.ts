import type { OrderState } from '@seatlock/shared';
import type { NewOrder, Order, OrdersRepo, Queryable } from './types.js';

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

const COLUMNS =
  'id, hold_id, event_id, user_id, quantity, amount_cents, status, payment_id, created_at, updated_at';

function toOrder(row: OrderRow): Order {
  return {
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
  };
}

export class PgOrdersRepo implements OrdersRepo {
  constructor(private readonly db: Queryable) {}

  async insert(order: NewOrder): Promise<Order | null> {
    // UNIQUE(hold_id) is the last line of defence against two orders for one hold.
    const { rows } = await this.db.query<OrderRow>(
      `INSERT INTO orders (id, hold_id, event_id, user_id, quantity, amount_cents, status)
       VALUES ($1, $2, $3, $4, $5, $6, 'PENDING_PAYMENT')
       ON CONFLICT (hold_id) DO NOTHING
       RETURNING ${COLUMNS}`,
      [order.id, order.holdId, order.eventId, order.userId, order.quantity, order.amountCents],
    );
    return rows[0] ? toOrder(rows[0]) : null;
  }

  async findById(id: string): Promise<Order | null> {
    const { rows } = await this.db.query<OrderRow>(`SELECT ${COLUMNS} FROM orders WHERE id = $1`, [
      id,
    ]);
    return rows[0] ? toOrder(rows[0]) : null;
  }

  async findByIdForUpdate(id: string): Promise<Order | null> {
    const { rows } = await this.db.query<OrderRow>(
      `SELECT ${COLUMNS} FROM orders WHERE id = $1 FOR UPDATE`,
      [id],
    );
    return rows[0] ? toOrder(rows[0]) : null;
  }

  async findByHoldIdForUpdate(holdId: string): Promise<Order | null> {
    const { rows } = await this.db.query<OrderRow>(
      `SELECT ${COLUMNS} FROM orders WHERE hold_id = $1 FOR UPDATE`,
      [holdId],
    );
    return rows[0] ? toOrder(rows[0]) : null;
  }

  async listByUser(userId: string, limit: number, offset: number): Promise<Order[]> {
    const { rows } = await this.db.query<OrderRow>(
      `SELECT ${COLUMNS} FROM orders
       WHERE user_id = $1
       ORDER BY created_at DESC, id DESC
       LIMIT $2 OFFSET $3`,
      [userId, limit, offset],
    );
    return rows.map(toOrder);
  }

  async setPaymentId(id: string, paymentId: string): Promise<Order | null> {
    const { rows } = await this.db.query<OrderRow>(
      `UPDATE orders SET payment_id = $2, updated_at = NOW() WHERE id = $1 RETURNING ${COLUMNS}`,
      [id, paymentId],
    );
    return rows[0] ? toOrder(rows[0]) : null;
  }

  async updateStatus(id: string, from: OrderState, to: OrderState): Promise<Order | null> {
    // `AND status = $2` makes this a compare-and-set, even if a caller forgot to lock the row.
    const { rows } = await this.db.query<OrderRow>(
      `UPDATE orders SET status = $3, updated_at = NOW()
       WHERE id = $1 AND status = $2
       RETURNING ${COLUMNS}`,
      [id, from, to],
    );
    return rows[0] ? toOrder(rows[0]) : null;
  }
}
