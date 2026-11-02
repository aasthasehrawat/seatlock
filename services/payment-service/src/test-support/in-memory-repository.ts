import type {
  NewPayment,
  OutboxMessage,
  Payment,
  PaymentRepository,
  PaymentResult,
  PaymentTx,
} from '../payments/types.js';

/**
 * In-memory PaymentRepository for unit tests.
 *
 * Transactions are serialised through a promise queue, which mimics the effect of
 * row locks closely enough to test races. There is no rollback: tests must not
 * rely on a failed transaction undoing its writes.
 */
export class InMemoryPaymentRepository implements PaymentRepository {
  readonly payments = new Map<string, Payment>();
  readonly processedEvents = new Set<string>();
  readonly outbox: OutboxMessage[] = [];

  private queue: Promise<unknown> = Promise.resolve();
  private tick = 0;

  private readonly tx: PaymentTx = {
    lockById: async (id) => this.payments.get(id) ?? null,

    findByOrderId: async (orderId) => this.byOrder(orderId),

    insertPendingIfAbsent: async (payment: NewPayment) => {
      if (this.payments.has(payment.id) || this.byOrder(payment.orderId)) return;
      const now = this.now();
      this.payments.set(payment.id, {
        ...payment,
        status: 'PENDING',
        providerRef: null,
        failureReason: null,
        attemptCount: 0,
        createdAt: now,
        updatedAt: now,
      });
    },

    recordResult: async (id: string, result: PaymentResult) => {
      const current = this.payments.get(id);
      if (!current) throw new Error(`Payment ${id} not found`);
      const updated: Payment = {
        ...current,
        status: result.status,
        providerRef: result.providerRef,
        failureReason: result.failureReason,
        attemptCount: current.attemptCount + 1,
        updatedAt: this.now(),
      };
      this.payments.set(id, updated);
      return updated;
    },

    enqueueOutbox: async (message) => {
      if (this.outbox.some((m) => m.eventId === message.eventId)) return;
      this.outbox.push(message);
    },

    markEventProcessed: async (eventId) => {
      if (this.processedEvents.has(eventId)) return false;
      this.processedEvents.add(eventId);
      return true;
    },
  };

  withTransaction<T>(fn: (tx: PaymentTx) => Promise<T>): Promise<T> {
    const run = this.queue.then(() => fn(this.tx));
    this.queue = run.catch(() => undefined);
    return run;
  }

  async isEventProcessed(eventId: string): Promise<boolean> {
    return this.processedEvents.has(eventId);
  }

  async findById(id: string): Promise<Payment | null> {
    return this.payments.get(id) ?? null;
  }

  async findByOrderId(orderId: string): Promise<Payment | null> {
    return this.byOrder(orderId);
  }

  async list(filter: { orderId?: string; limit: number }): Promise<Payment[]> {
    return [...this.payments.values()]
      .filter((p) => !filter.orderId || p.orderId === filter.orderId)
      .slice(0, filter.limit);
  }

  /** Outbox messages of one event type, in insertion order. */
  outboxOfType(type: string): OutboxMessage[] {
    return this.outbox.filter((m) => m.envelope.type === type);
  }

  private byOrder(orderId: string): Payment | null {
    for (const payment of this.payments.values()) {
      if (payment.orderId === orderId) return payment;
    }
    return null;
  }

  private now(): string {
    this.tick += 1;
    return new Date(Date.UTC(2026, 0, 1, 0, 0, this.tick)).toISOString();
  }
}
