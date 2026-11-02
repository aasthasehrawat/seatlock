import type { FastifyPluginAsync } from 'fastify';
import { NotFoundError } from '@seatlock/shared';
import { z } from 'zod';
import type { Payment, PaymentRepository } from '../payments/types.js';
import { forcePaymentResult } from '../workers/payment-processor.js';

export interface PaymentRouteOptions {
  repo: PaymentRepository;
  producerName: string;
  enableSimulateRoute: boolean;
}

const IdParams = z.object({ id: z.string().uuid() });

const ListQuery = z.object({
  orderId: z.string().uuid().optional(),
  limit: z.coerce.number().int().min(1).max(200).default(50),
});

const SimulateBody = z.object({
  outcome: z.enum(['succeed', 'fail']),
  reason: z.string().min(1).max(200).optional(),
  force: z.boolean().optional(),
});

/** Public JSON shape of a payment. */
export function toPaymentDto(payment: Payment) {
  return {
    id: payment.id,
    orderId: payment.orderId,
    holdId: payment.holdId,
    userId: payment.userId,
    amountCents: payment.amountCents,
    currency: payment.currency,
    status: payment.status,
    providerRef: payment.providerRef,
    failureReason: payment.failureReason,
    attemptCount: payment.attemptCount,
    createdAt: payment.createdAt,
    updatedAt: payment.updatedAt,
  };
}

export const paymentRoutes: FastifyPluginAsync<PaymentRouteOptions> = async (app, opts) => {
  app.get('/payments/:id', async (request) => {
    const { id } = IdParams.parse(request.params);
    const payment = await opts.repo.findById(id);
    if (!payment) throw new NotFoundError('Payment', id);
    return toPaymentDto(payment);
  });

  app.get('/payments', async (request) => {
    const query = ListQuery.parse(request.query);
    const payments = await opts.repo.list({
      limit: query.limit,
      ...(query.orderId ? { orderId: query.orderId } : {}),
    });
    return { payments: payments.map(toPaymentDto) };
  });

  if (opts.enableSimulateRoute) {
    // Debug only: force a payment to succeed or fail and publish the matching event.
    app.post('/payments/:id/simulate', async (request) => {
      const { id } = IdParams.parse(request.params);
      const body = SimulateBody.parse(request.body ?? {});
      const payment = await forcePaymentResult({
        repo: opts.repo,
        producerName: opts.producerName,
        paymentId: id,
        outcome: body.outcome,
        reason: body.reason,
        force: body.force,
      });
      return toPaymentDto(payment);
    });
  }
};
