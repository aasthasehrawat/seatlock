import {
  EVENT_TYPES,
  IdempotencyConflictError,
  NotFoundError,
  ORDER_STATES,
  assertOrderTransition,
  canTransitionOrder,
  createId,
  type HoldExpiredPayload,
  type InventoryReleasedPayload,
  type InventoryReservedPayload,
  type OrderState,
  type PaymentFailedPayload,
  type PaymentSucceededPayload,
} from '@seatlock/shared';
import type { Order, TxContext, UnitOfWork } from '../db/types.js';
import { NonRetryableError, OrderNotPayableError, PaymentAlreadyInitiatedError } from '../lib/errors.js';
import { hashRequest } from '../lib/hash.js';
import type { Logger } from '../lib/logger.js';
import { buildOutboxRecord } from '../lib/outbox-events.js';

/** Name under which this service records processed events (processed_events.consumer_name). */
export const CONSUMER_NAME = 'order-service';

/** orders.amount_cents is a Postgres INT (int4). */
const MAX_INT4 = 2_147_483_647;

/** The states an order can move *to* (PENDING_PAYMENT is only ever an initial state). */
type TerminalOrderState = Exclude<OrderState, typeof ORDER_STATES.PENDING_PAYMENT>;

// ---------------------------------------------------------------------------
// Public types
// ---------------------------------------------------------------------------

/** The slice of the Kafka envelope the service needs: dedupe key + trace id. */
export interface EventMeta {
  eventId: string;
  correlationId: string;
}

/**
 * - applied:   state changed and events were written to the outbox
 * - duplicate: this exact message (eventId) was already processed; nothing done
 * - ignored:   a new message, but not applicable (stale, out of order, already terminal)
 */
export type EventOutcome = 'applied' | 'duplicate' | 'ignored';

export interface EventResult {
  outcome: EventOutcome;
  reason?: string;
  orderId?: string;
}

export interface InitiatePaymentCommand {
  orderId: string;
  idempotencyKey: string;
  currency?: string;
  correlationId?: string;
}

export interface PaymentInitiationBody {
  orderId: string;
  paymentId: string;
  /** The ORDER status; payment outcome arrives asynchronously via payments.events. */
  status: OrderState;
  paymentStatus: 'REQUESTED';
  amountCents: number;
  currency: string;
}

export interface PaymentInitiationResult {
  statusCode: number;
  body: PaymentInitiationBody;
  /** True when this response was replayed from a previous request with the same key. */
  replayed: boolean;
}

export interface OrderServiceDeps {
  uow: UnitOfWork;
  logger: Logger;
  defaultCurrency?: string;
  /** Injectable for deterministic tests. */
  newId?: () => string;
  consumerName?: string;
}

const ignored = (reason: string, orderId?: string): EventResult => ({
  outcome: 'ignored',
  reason,
  orderId,
});

// ---------------------------------------------------------------------------
// Service
// ---------------------------------------------------------------------------

/**
 * Order lifecycle + state machine.
 *
 *            InventoryReserved
 *                   |
 *                   v
 *            PENDING_PAYMENT --PaymentSucceeded--> CONFIRMED
 *                   |  \
 *                   |   `-----PaymentFailed-------> FAILED
 *                   `---------hold expired--------> EXPIRED
 *
 * Every mutation follows the same recipe, inside ONE database transaction:
 *   1. (consumers) claim the event in processed_events -> dedupe
 *   2. lock the order row (FOR UPDATE)               -> serialise concurrent writers
 *   3. validate the transition (shared state machine)
 *   4. update the order
 *   5. write the resulting domain event to the outbox  -> publish atomically, via the relay
 * If any step throws, the whole transaction rolls back, including the dedupe claim, so a retry
 * starts from a clean slate.
 */
export class OrderService {
  private readonly uow: UnitOfWork;
  private readonly logger: Logger;
  private readonly defaultCurrency: string;
  private readonly newId: () => string;
  private readonly consumerName: string;

  constructor(deps: OrderServiceDeps) {
    this.uow = deps.uow;
    this.logger = deps.logger.child({ component: 'order-service' });
    this.defaultCurrency = (deps.defaultCurrency ?? 'USD').toUpperCase();
    this.newId = deps.newId ?? createId;
    this.consumerName = deps.consumerName ?? CONSUMER_NAME;
  }

  // -------------------------------------------------------------------------
  // Queries
  // -------------------------------------------------------------------------

  async getOrder(id: string): Promise<Order> {
    const order = await this.uow.read((ctx) => ctx.orders.findById(id));
    if (!order) throw new NotFoundError('Order', id);
    return order;
  }

  async listOrders(userId: string, limit: number, offset: number): Promise<Order[]> {
    return this.uow.read((ctx) => ctx.orders.listByUser(userId, limit, offset));
  }

  // -------------------------------------------------------------------------
  // Command: start payment (HTTP)
  // -------------------------------------------------------------------------

  /**
   * Start a payment for an order. Safe to retry with the same Idempotency-Key.
   *
   * The idempotency record is written in the SAME transaction as the side effects (payment id,
   * PaymentRequested outbox row), so there is no window where the work committed but the key
   * was not recorded (a retry would double-charge) or vice versa (a retry would be wrongly
   * replayed). A per-key advisory lock makes concurrent retries queue up instead of racing.
   *
   * Only successful outcomes are stored. A failed attempt (404, 409) rolls back and the key stays
   * free, so the client can retry the same key once the problem is fixed.
   */
  async initiatePayment(cmd: InitiatePaymentCommand): Promise<PaymentInitiationResult> {
    const currency = (cmd.currency ?? this.defaultCurrency).toUpperCase();
    const requestHash = hashRequest({ orderId: cmd.orderId, currency });

    return this.uow.run<PaymentInitiationResult>(async (ctx) => {
      await ctx.idempotency.lock(cmd.idempotencyKey);

      const existing = await ctx.idempotency.find(cmd.idempotencyKey);
      if (existing) {
        if (existing.requestHash !== requestHash) throw new IdempotencyConflictError();
        return {
          statusCode: existing.responseStatus,
          body: existing.responseBody as PaymentInitiationBody,
          replayed: true,
        };
      }

      const order = await ctx.orders.findByIdForUpdate(cmd.orderId);
      if (!order) throw new NotFoundError('Order', cmd.orderId);

      if (order.status !== ORDER_STATES.PENDING_PAYMENT) {
        throw new OrderNotPayableError(order.id, order.status);
      }
      if (order.paymentId !== null) {
        // Different key, but a payment is already in flight: refuse rather than double-charge.
        throw new PaymentAlreadyInitiatedError(order.id);
      }

      const paymentId = this.newId();
      await ctx.orders.setPaymentId(order.id, paymentId);

      await ctx.outbox.insert(
        buildOutboxRecord({
          type: EVENT_TYPES.PaymentRequested,
          orderId: order.id,
          correlationId: cmd.correlationId ?? order.holdId,
          payload: {
            paymentId,
            orderId: order.id,
            holdId: order.holdId,
            userId: order.userId,
            amountCents: order.amountCents,
            currency,
          },
        }),
      );

      const body: PaymentInitiationBody = {
        orderId: order.id,
        paymentId,
        status: order.status,
        paymentStatus: 'REQUESTED',
        amountCents: order.amountCents,
        currency,
      };
      const statusCode = 202; // Accepted: the outcome is asynchronous.

      await ctx.idempotency.save({
        key: cmd.idempotencyKey,
        requestHash,
        responseStatus: statusCode,
        responseBody: body,
      });

      this.logger.info({ orderId: order.id, paymentId }, 'payment requested');
      return { statusCode, body, replayed: false };
    });
  }

  // -------------------------------------------------------------------------
  // Event handlers (Kafka)
  // -------------------------------------------------------------------------

  /** inventory.events / InventoryReserved: a hold exists, so create the order awaiting payment. */
  async onInventoryReserved(meta: EventMeta, payload: InventoryReservedPayload): Promise<EventResult> {
    return this.consumeOnce(meta, async (ctx) => {
      // Integer cents only; guard against overflowing the INT column rather than failing in SQL.
      const amountCents = payload.quantity * payload.unitPriceCents;
      if (!Number.isSafeInteger(amountCents) || amountCents > MAX_INT4) {
        throw new NonRetryableError(
          `Order amount out of range for hold ${payload.holdId}: ${payload.quantity} x ${payload.unitPriceCents}`,
        );
      }

      const order = await ctx.orders.insert({
        id: this.newId(),
        holdId: payload.holdId,
        eventId: payload.eventId,
        userId: payload.userId,
        quantity: payload.quantity,
        amountCents,
      });

      // UNIQUE(hold_id): the hold already has an order (e.g. the producer re-emitted the event
      // with a new eventId). Idempotent no-op.
      if (!order) return ignored('order_already_exists_for_hold');

      await ctx.outbox.insert(
        buildOutboxRecord({
          type: EVENT_TYPES.OrderCreated,
          orderId: order.id,
          correlationId: meta.correlationId,
          payload: {
            orderId: order.id,
            holdId: order.holdId,
            eventId: order.eventId,
            userId: order.userId,
            quantity: order.quantity,
            amountCents: order.amountCents,
          },
        }),
      );

      this.logger.info({ orderId: order.id, holdId: order.holdId }, 'order created');
      return { outcome: 'applied', orderId: order.id };
    });
  }

  /** payments.events / PaymentSucceeded: PENDING_PAYMENT -> CONFIRMED. */
  async onPaymentSucceeded(meta: EventMeta, payload: PaymentSucceededPayload): Promise<EventResult> {
    return this.consumeOnce(meta, async (ctx) => {
      const order = await ctx.orders.findByIdForUpdate(payload.orderId);
      // Payments only exist for orders we created, so a missing order is abnormal. Throwing sends
      // the message through retry (covers replica lag / ordering oddities) and finally the DLQ.
      if (!order) throw new NotFoundError('Order', payload.orderId);

      // A result for a payment we did not request for this order is stale: ignore it.
      if (order.paymentId !== payload.paymentId) return ignored('payment_id_mismatch', order.id);

      if (!canTransitionOrder(order.status, ORDER_STATES.CONFIRMED)) {
        if (order.status === ORDER_STATES.CONFIRMED) return ignored('already_confirmed', order.id);
        // Money was taken for an order that is already FAILED/EXPIRED. The state machine rightly
        // forbids resurrecting it; this needs a refund, so shout about it.
        this.logger.warn(
          { orderId: order.id, paymentId: payload.paymentId, status: order.status },
          'late payment success for a terminal order: refund required',
        );
        return ignored(`late_payment_for_${order.status.toLowerCase()}_order`, order.id);
      }

      if (payload.amountCents !== order.amountCents) {
        // Charged amount differs from the order total: a broken invariant, not a transient error.
        throw new NonRetryableError(
          `Payment ${payload.paymentId} amount ${payload.amountCents} != order ${order.id} amount ${order.amountCents}`,
        );
      }

      const confirmed = await this.transition(ctx, order, ORDER_STATES.CONFIRMED);
      await ctx.outbox.insert(
        buildOutboxRecord({
          type: EVENT_TYPES.OrderConfirmed,
          orderId: confirmed.id,
          correlationId: meta.correlationId,
          payload: {
            orderId: confirmed.id,
            holdId: confirmed.holdId,
            eventId: confirmed.eventId,
            userId: confirmed.userId,
            quantity: confirmed.quantity,
            amountCents: confirmed.amountCents,
            paymentId: payload.paymentId,
          },
        }),
      );

      this.logger.info({ orderId: confirmed.id }, 'order confirmed');
      return { outcome: 'applied', orderId: confirmed.id };
    });
  }

  /** payments.events / PaymentFailed: PENDING_PAYMENT -> FAILED. */
  async onPaymentFailed(meta: EventMeta, payload: PaymentFailedPayload): Promise<EventResult> {
    return this.consumeOnce(meta, async (ctx) => {
      const order = await ctx.orders.findByIdForUpdate(payload.orderId);
      if (!order) throw new NotFoundError('Order', payload.orderId);

      if (order.paymentId !== payload.paymentId) return ignored('payment_id_mismatch', order.id);

      // A failure for an order that is already terminal changes nothing (e.g. expired meanwhile).
      if (!canTransitionOrder(order.status, ORDER_STATES.FAILED)) {
        return ignored(`order_already_${order.status.toLowerCase()}`, order.id);
      }

      const failed = await this.transition(ctx, order, ORDER_STATES.FAILED);
      await ctx.outbox.insert(
        buildOutboxRecord({
          type: EVENT_TYPES.OrderFailed,
          orderId: failed.id,
          correlationId: meta.correlationId,
          payload: {
            orderId: failed.id,
            holdId: failed.holdId,
            eventId: failed.eventId,
            reason: payload.reason,
          },
        }),
      );

      this.logger.info({ orderId: failed.id, reason: payload.reason }, 'order failed');
      return { outcome: 'applied', orderId: failed.id };
    });
  }

  /**
   * inventory.events / InventoryReleased. Only reason EXPIRED matters here: the hold timed out,
   * so an order still awaiting payment can never be fulfilled. Other reasons are either our own
   * cascade (PAYMENT_FAILED after OrderFailed) or not an order-service concern.
   */
  async onInventoryReleased(meta: EventMeta, payload: InventoryReleasedPayload): Promise<EventResult> {
    if (payload.reason !== 'EXPIRED') {
      return this.consumeOnce(meta, async () => ignored(`release_reason_${payload.reason.toLowerCase()}`));
    }
    return this.expireOrderForHold(meta, payload.holdId);
  }

  /** inventory.events / HoldExpired: same effect as InventoryReleased(EXPIRED). */
  async onHoldExpired(meta: EventMeta, payload: HoldExpiredPayload): Promise<EventResult> {
    return this.expireOrderForHold(meta, payload.holdId);
  }

  // -------------------------------------------------------------------------
  // Internals
  // -------------------------------------------------------------------------

  private async expireOrderForHold(meta: EventMeta, holdId: string): Promise<EventResult> {
    return this.consumeOnce(meta, async (ctx) => {
      const order = await ctx.orders.findByHoldIdForUpdate(holdId);
      // The hold expired before we ever created an order (or it was never ours): nothing to do.
      if (!order) return ignored('no_order_for_hold');

      if (!canTransitionOrder(order.status, ORDER_STATES.EXPIRED)) {
        return ignored(`order_already_${order.status.toLowerCase()}`, order.id);
      }

      const expired = await this.transition(ctx, order, ORDER_STATES.EXPIRED);
      await ctx.outbox.insert(
        buildOutboxRecord({
          type: EVENT_TYPES.OrderExpired,
          orderId: expired.id,
          correlationId: meta.correlationId,
          payload: {
            orderId: expired.id,
            holdId: expired.holdId,
            eventId: expired.eventId,
          },
        }),
      );

      this.logger.info({ orderId: expired.id, holdId }, 'order expired');
      return { outcome: 'applied', orderId: expired.id };
    });
  }

  /**
   * Consumer-side idempotency. Claim the event inside the same transaction as the work: a
   * redelivery finds the claim and is skipped, while a failure rolls the claim back so the
   * retry is processed normally.
   */
  private consumeOnce(
    meta: EventMeta,
    work: (ctx: TxContext) => Promise<EventResult>,
  ): Promise<EventResult> {
    return this.uow.run<EventResult>(async (ctx) => {
      const firstTime = await ctx.processedEvents.markProcessed(this.consumerName, meta.eventId);
      if (!firstTime) return { outcome: 'duplicate' };
      return work(ctx);
    });
  }

  /**
   * The single place an order's status changes. The shared state machine is the authority on
   * what is legal; the repo's compare-and-set guards against a lost update if a caller ever
   * forgets to lock the row.
   */
  private async transition(ctx: TxContext, order: Order, to: TerminalOrderState): Promise<Order> {
    assertOrderTransition(order.status, to);

    const updated = await ctx.orders.updateStatus(order.id, order.status, to);
    if (!updated) {
      // Plain Error => retryable: the redelivery will re-read the fresh state.
      throw new Error(`Order ${order.id} changed concurrently while moving ${order.status} -> ${to}`);
    }
    return updated;
  }
}
