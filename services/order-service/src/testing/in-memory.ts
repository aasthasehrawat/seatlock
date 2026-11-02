import type { OrderState } from '@seatlock/shared';
import type {
  IdempotencyRecord,
  NewOrder,
  Order,
  OutboxRecord,
  OutboxRow,
  TxContext,
  UnitOfWork,
} from '../db/types.js';

/**
 * Test double for the database. It honours the same contracts as the Postgres repositories
 * (unique hold_id, compare-and-set status, dedupe on processed_events) and, importantly, gives
 * `run()` real rollback semantics by snapshotting state, so tests can assert atomicity:
 * "if the handler throws, neither the order change NOR the outbox row NOR the dedupe claim stick".
 *
 * It does not emulate locks or concurrency; that is covered by the Postgres integration tests.
 */
interface State {
  orders: Map<string, Order>;
  outbox: OutboxRow[];
  idempotency: Map<string, IdempotencyRecord & { createdAt: Date }>;
  processed: Set<string>;
}

const emptyState = (): State => ({
  orders: new Map(),
  outbox: [],
  idempotency: new Map(),
  processed: new Set(),
});

export class InMemoryUnitOfWork implements UnitOfWork {
  private state: State = emptyState();

  // ---- inspection helpers for assertions ----

  get orders(): Order[] {
    return [...this.state.orders.values()];
  }

  getOrder(id: string): Order | undefined {
    return this.state.orders.get(id);
  }

  get outbox(): OutboxRow[] {
    return this.state.outbox;
  }

  outboxOfType(eventType: string): OutboxRow[] {
    return this.state.outbox.filter((r) => r.eventType === eventType);
  }

  get processedCount(): number {
    return this.state.processed.size;
  }

  get idempotencyCount(): number {
    return this.state.idempotency.size;
  }

  /** Test setup shortcut: force an order into a given status without going through the service. */
  forceStatus(id: string, status: OrderState): void {
    const order = this.state.orders.get(id);
    if (!order) throw new Error(`no such order ${id}`);
    this.state.orders.set(id, { ...order, status });
  }

  // ---- UnitOfWork ----

  async run<T>(fn: (ctx: TxContext) => Promise<T>): Promise<T> {
    const snapshot: State = {
      orders: new Map([...this.state.orders].map(([k, v]) => [k, { ...v }])),
      outbox: this.state.outbox.map((r) => ({ ...r })),
      idempotency: new Map([...this.state.idempotency].map(([k, v]) => [k, { ...v }])),
      processed: new Set(this.state.processed),
    };

    try {
      return await fn(this.context());
    } catch (err) {
      this.state = snapshot; // ROLLBACK
      throw err;
    }
  }

  async read<T>(fn: (ctx: TxContext) => Promise<T>): Promise<T> {
    return fn(this.context());
  }

  private context(): TxContext {
    // Repos read `this.state` lazily so a rollback (which swaps the state object) is respected.
    // eslint-disable-next-line @typescript-eslint/no-this-alias
    const self = this;

    return {
      orders: {
        async insert(order: NewOrder) {
          const exists = [...self.state.orders.values()].some((o) => o.holdId === order.holdId);
          if (exists) return null;
          const now = new Date();
          const created: Order = {
            ...order,
            status: 'PENDING_PAYMENT',
            paymentId: null,
            createdAt: now,
            updatedAt: now,
          };
          self.state.orders.set(order.id, created);
          return { ...created };
        },
        async findById(id) {
          const o = self.state.orders.get(id);
          return o ? { ...o } : null;
        },
        async findByIdForUpdate(id) {
          const o = self.state.orders.get(id);
          return o ? { ...o } : null;
        },
        async findByHoldIdForUpdate(holdId) {
          const o = [...self.state.orders.values()].find((x) => x.holdId === holdId);
          return o ? { ...o } : null;
        },
        async listByUser(userId, limit, offset) {
          return [...self.state.orders.values()]
            .filter((o) => o.userId === userId)
            .sort((a, b) => b.createdAt.getTime() - a.createdAt.getTime())
            .slice(offset, offset + limit)
            .map((o) => ({ ...o }));
        },
        async setPaymentId(id, paymentId) {
          const o = self.state.orders.get(id);
          if (!o) return null;
          const updated = { ...o, paymentId, updatedAt: new Date() };
          self.state.orders.set(id, updated);
          return { ...updated };
        },
        async updateStatus(id, from, to) {
          const o = self.state.orders.get(id);
          if (!o || o.status !== from) return null;
          const updated = { ...o, status: to, updatedAt: new Date() };
          self.state.orders.set(id, updated);
          return { ...updated };
        },
      },

      outbox: {
        async insert(record: OutboxRecord) {
          self.state.outbox.push({
            ...record,
            headers: { ...record.headers },
            published: false,
            createdAt: new Date(),
            publishedAt: null,
          });
        },
        async tryAcquireRelayLock() {
          return true;
        },
        async fetchUnpublished(limit) {
          return self.state.outbox
            .filter((r) => !r.published)
            .slice(0, limit)
            .map((r) => ({ ...r }));
        },
        async markPublished(ids) {
          for (const row of self.state.outbox) {
            if (ids.includes(row.id)) {
              row.published = true;
              row.publishedAt = new Date();
            }
          }
        },
        async purgePublishedBefore(cutoff) {
          const before = self.state.outbox.length;
          self.state.outbox = self.state.outbox.filter(
            (r) => !(r.published && r.publishedAt && r.publishedAt < cutoff),
          );
          return before - self.state.outbox.length;
        },
      },

      idempotency: {
        async lock() {
          // Single-threaded in tests; the Postgres version takes an advisory lock.
        },
        async find(key) {
          const r = self.state.idempotency.get(key);
          return r
            ? {
                key: r.key,
                requestHash: r.requestHash,
                responseStatus: r.responseStatus,
                responseBody: r.responseBody,
              }
            : null;
        },
        async save(record) {
          if (!self.state.idempotency.has(record.key)) {
            self.state.idempotency.set(record.key, { ...record, createdAt: new Date() });
          }
        },
        async purgeBefore(cutoff) {
          let removed = 0;
          for (const [key, r] of self.state.idempotency) {
            if (r.createdAt < cutoff) {
              self.state.idempotency.delete(key);
              removed += 1;
            }
          }
          return removed;
        },
      },

      processedEvents: {
        async markProcessed(consumerName, eventId) {
          const id = `${consumerName}:${eventId}`;
          if (self.state.processed.has(id)) return false;
          self.state.processed.add(id);
          return true;
        },
      },
    };
  }
}
