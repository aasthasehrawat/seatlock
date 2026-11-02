import type { EventEnvelope } from '@seatlock/shared';

export type PaymentStatus = 'PENDING' | 'SUCCEEDED' | 'FAILED';

export interface Payment {
  id: string;
  orderId: string;
  holdId: string;
  userId: string;
  amountCents: number;
  currency: string;
  status: PaymentStatus;
  providerRef: string | null;
  failureReason: string | null;
  attemptCount: number;
  createdAt: string;
  updatedAt: string;
}

export interface NewPayment {
  id: string;
  orderId: string;
  holdId: string;
  userId: string;
  amountCents: number;
  currency: string;
}

export interface PaymentResult {
  status: 'SUCCEEDED' | 'FAILED';
  providerRef: string | null;
  failureReason: string | null;
}

/** A message waiting in the transactional outbox to be published to Kafka. */
export interface OutboxMessage {
  eventId: string;
  topic: string;
  key: string;
  envelope: EventEnvelope;
}

/** Operations that run inside one database transaction. */
export interface PaymentTx {
  /** Row-lock the payment (SELECT ... FOR UPDATE). */
  lockById(id: string): Promise<Payment | null>;
  findByOrderId(orderId: string): Promise<Payment | null>;
  /** Insert a PENDING payment unless one already exists for the order (or id). */
  insertPendingIfAbsent(payment: NewPayment): Promise<void>;
  /** Persist a terminal result and bump attempt_count. */
  recordResult(id: string, result: PaymentResult): Promise<Payment>;
  enqueueOutbox(message: OutboxMessage): Promise<void>;
  /** Returns false if this event was already recorded (i.e. it is a duplicate). */
  markEventProcessed(eventId: string, eventType: string): Promise<boolean>;
}

export interface PaymentRepository {
  withTransaction<T>(fn: (tx: PaymentTx) => Promise<T>): Promise<T>;
  isEventProcessed(eventId: string): Promise<boolean>;
  findById(id: string): Promise<Payment | null>;
  findByOrderId(orderId: string): Promise<Payment | null>;
  list(filter: { orderId?: string; limit: number }): Promise<Payment[]>;
}
