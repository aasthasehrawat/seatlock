import type { FastifyPluginAsync } from 'fastify';
import { ValidationError } from '@seatlock/shared';
import { z } from 'zod';
import type { Order } from '../db/types.js';
import type { OrderService } from '../services/order-service.js';

const IdParamsSchema = z.object({ id: z.string().uuid() });

const ListQuerySchema = z.object({
  userId: z.string().uuid(),
  limit: z.coerce.number().int().min(1).max(100).default(50),
  offset: z.coerce.number().int().min(0).default(0),
});

const PayBodySchema = z
  .object({
    currency: z
      .string()
      .regex(/^[A-Za-z]{3}$/, 'currency must be a 3-letter ISO code')
      .optional(),
  })
  .strict();

const IdempotencyKeySchema = z.string().min(1).max(255);

/** API representation of an order: ISO timestamps, camelCase, no DB leakage. */
export function toOrderDto(order: Order) {
  return {
    id: order.id,
    holdId: order.holdId,
    eventId: order.eventId,
    userId: order.userId,
    quantity: order.quantity,
    amountCents: order.amountCents,
    status: order.status,
    paymentId: order.paymentId,
    createdAt: order.createdAt.toISOString(),
    updatedAt: order.updatedAt.toISOString(),
  };
}

export interface OrderRoutesOptions {
  service: OrderService;
}

export const orderRoutes: FastifyPluginAsync<OrderRoutesOptions> = async (app, { service }) => {
  app.get('/orders/:id', async (request) => {
    const { id } = IdParamsSchema.parse(request.params);
    return toOrderDto(await service.getOrder(id));
  });

  app.get('/orders', async (request) => {
    const { userId, limit, offset } = ListQuerySchema.parse(request.query);
    const orders = await service.listOrders(userId, limit, offset);
    return { items: orders.map(toOrderDto), limit, offset };
  });

  app.post('/orders/:id/pay', async (request, reply) => {
    const { id } = IdParamsSchema.parse(request.params);

    // Required: without a key a client cannot safely retry a payment after a timeout.
    const keyResult = IdempotencyKeySchema.safeParse(request.headers['idempotency-key']);
    if (!keyResult.success) {
      throw new ValidationError('Idempotency-Key header is required (1-255 characters)');
    }

    const body = PayBodySchema.parse(request.body ?? {});
    const correlationHeader = request.headers['x-correlation-id'];

    const result = await service.initiatePayment({
      orderId: id,
      idempotencyKey: keyResult.data,
      currency: body.currency,
      correlationId: typeof correlationHeader === 'string' ? correlationHeader : undefined,
    });

    if (result.replayed) reply.header('Idempotent-Replayed', 'true');
    return reply.status(result.statusCode).send(result.body);
  });
};
