import type { QueryResult, QueryResultRow } from 'pg';
import type { EventType, OrderState } from '@seatlock/shared';

/**
 * Minimal query surface shared by `pg.Pool` and `pg.PoolClient`. Repositories depend on this
 * (not on pg directly), which keeps them transaction-agnostic and trivially fakeable.
 */
export interface Queryable {
  query<R extends QueryResultRow = QueryResultRow>(
    text: string,
    values?: unknown[],
  ): Promise<QueryResult<R>>;
}

// ---------------------------------------------------------------------------
// Domain records
// ---------------------------------------------------------------------------

export interface Order {
  id: string;
  holdId: string;
  eventId: string;
  userId: string;
  quantity: number;
  amountCents: number;
  status: OrderState;
  paymentId: string | null;
  createdAt: Date;
  updatedAt: Date;
}

export interface NewOrder {
  id: string;
  holdId: string;
  eventId: string;
  userId: string;
  quantity: number;
  amountCents: number;
}

/** Envelope metadata persisted with an outbox row so the relay can rebuild the exact envelope. */
export interface OutboxHeaders {
  correlationId: string;
  producer: string;
  version: number;
  occurredAt: string;
  /** Kafka message key. Same key => same partition => per-order ordering. */
  partitionKey: string;
  [key: string]: unknown;
}

/** What the service writes. `id` doubles as the Kafka envelope `eventId` (consumer dedupe key). */
export interface OutboxRecord {
  id: string;
  aggregateType: string;
  aggregateId: string;
  eventType: EventType;
  payload: unknown;
  headers: OutboxHeaders;
}

/** What the relay reads back. Headers are loosely typed because they come from JSONB. */
export interface OutboxRow {
  id: string;
  aggregateType: string;
  aggregateId: string;
  eventType: EventType;
  payload: unknown;
  headers: Partial<OutboxHeaders>;
  published: boolean;
  createdAt: Date;
  publishedAt: Date | null;
}

export interface IdempotencyRecord {
  key: string;
  requestHash: string;
  responseStatus: number;
  responseBody: unknown;
}

// ---------------------------------------------------------------------------
// Repository contracts
// ---------------------------------------------------------------------------

export interface OrdersRepo {
  /** Insert a PENDING_PAYMENT order. Returns null if an order for this hold already exists. */
  insert(order: NewOrder): Promise<Order | null>;
  findById(id: string): Promise<Order | null>;
  /** `SELECT ... FOR UPDATE`: serialises concurrent writers on the same order. */
  findByIdForUpdate(id: string): Promise<Order | null>;
  findByHoldIdForUpdate(holdId: string): Promise<Order | null>;
  listByUser(userId: string, limit: number, offset: number): Promise<Order[]>;
  setPaymentId(id: string, paymentId: string): Promise<Order | null>;
  /** Compare-and-set on status. Returns null if the order was not in `from`. */
  updateStatus(id: string, from: OrderState, to: OrderState): Promise<Order | null>;
}

export interface OutboxRepo {
  insert(record: OutboxRecord): Promise<void>;
  /**
   * Take the transaction-scoped relay lock. Only one relay instance may publish at a time,
   * which preserves outbox order. Released automatically on COMMIT/ROLLBACK.
   */
  tryAcquireRelayLock(): Promise<boolean>;
  fetchUnpublished(limit: number): Promise<OutboxRow[]>;
  markPublished(ids: string[]): Promise<void>;
  purgePublishedBefore(cutoff: Date): Promise<number>;
}

export interface IdempotencyRepo {
  /** Transaction-scoped advisory lock on the key: concurrent requests with one key run serially. */
  lock(key: string): Promise<void>;
  find(key: string): Promise<IdempotencyRecord | null>;
  save(record: IdempotencyRecord): Promise<void>;
  purgeBefore(cutoff: Date): Promise<number>;
}

export interface ProcessedEventsRepo {
  /** Returns true the first time (consumer, eventId) is seen, false for duplicates. */
  markProcessed(consumerName: string, eventId: string): Promise<boolean>;
}

/** Everything a unit of work can touch. All repos share one connection/transaction. */
export interface TxContext {
  orders: OrdersRepo;
  outbox: OutboxRepo;
  idempotency: IdempotencyRepo;
  processedEvents: ProcessedEventsRepo;
}

export interface UnitOfWork {
  /** Run `fn` in one DB transaction: COMMIT if it resolves, ROLLBACK if it throws. */
  run<T>(fn: (ctx: TxContext) => Promise<T>): Promise<T>;
  /** Run read-only queries without opening a transaction. */
  read<T>(fn: (ctx: TxContext) => Promise<T>): Promise<T>;
}
