# Event catalog

Every message on Kafka is a JSON **envelope** carrying one typed payload. The authoritative
definitions live in `packages/shared/src/events/` (`types.ts`, `envelope.ts`, `schemas.ts`,
`topics.ts`); this page documents them and who produces and consumes each one.

- [Envelope](#envelope)
- [Topics](#topics)
- [Summary matrix](#summary-matrix)
- [Inventory events](#inventory-events)
- [Payment events](#payment-events)
- [Order events](#order-events)
- [Notification events](#notification-events)
- [Kafka message headers](#kafka-message-headers)
- [DLQ and retry headers](#dlq-and-retry-headers)
- [Rules for evolving events](#rules-for-evolving-events)

## Envelope

```json
{
  "eventId": "7f0b7c1e-2d57-4a43-a0d4-6e0f6a8c9b11",
  "type": "InventoryReserved",
  "version": 1,
  "occurredAt": "2026-11-02T06:00:00.000Z",
  "correlationId": "3b6c0d58-...",
  "producer": "inventory-service",
  "payload": {}
}
```

| Field           | Meaning                                                                                                                                                                                               |
| --------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `eventId`       | UUID, unique **per message**. Consumers use it for idempotent processing (`processed_events`). Minted once, when the event is written to the outbox, so a re-published duplicate carries the same id. |
| `type`          | One of the 13 event types below.                                                                                                                                                                      |
| `version`       | Payload schema version (currently always `1`), so schemas can evolve safely.                                                                                                                          |
| `occurredAt`    | ISO-8601 UTC timestamp of when the event was created.                                                                                                                                                 |
| `correlationId` | Shared by all events in one user flow; also accepted from / returned in the `X-Correlation-Id` HTTP header.                                                                                           |
| `producer`      | Name of the producing service.                                                                                                                                                                        |
| `payload`       | Event-specific body, validated with the Zod schema for `type`.                                                                                                                                        |

> **Naming trap:** inside payloads, `eventId` means the **concert/event being sold** (an `events` row
> in inventory). The envelope's `eventId` is the **message id**. They are different things.

## Topics

| Topic                  | Partition key      | Produced by                                                   | Consumed by                                                             |
| ---------------------- | ------------------ | ------------------------------------------------------------- | ----------------------------------------------------------------------- |
| `inventory.events`     | concert (event) id | inventory-service                                             | order-service, notification-service                                     |
| `orders.events`        | order id           | order-service                                                 | inventory-service, notification-service                                 |
| `payments.events`      | order id           | order-service (`PaymentRequested`), payment-service (results) | payment-service, order-service, inventory-service, notification-service |
| `notifications.events` | n/a                | nobody yet                                                    | nobody yet                                                              |
| `retry.5s`             | original key       | inventory-service, order-service (on transient failure)       | the same service that parked it (others skip)                           |
| `retry.30s`            | n/a                | nobody yet                                                    | nobody yet                                                              |
| `dlq`                  | original key       | all four services                                             | humans (`npm run dlq:inspect`, `npm run dlq:replay`)                    |

`payments.events` is shared by a command and its results; each consumer ignores types it does not handle.

## Summary matrix

Legend: **P** produces, **C** consumes and acts, **-** consumes and ignores / not involved.

| Event                   | Topic                  | inventory |           order           | payment |        notification         | Notification template                       |
| ----------------------- | ---------------------- | :-------: | :-----------------------: | :-----: | :-------------------------: | ------------------------------------------- |
| `InventoryReserved`     | `inventory.events`     |     P     |             C             |    -    |              C              | `tickets_reserved`                          |
| `InventoryReleased`     | `inventory.events`     |     P     | C (only reason `EXPIRED`) |    -    |              -              |                                             |
| `InventoryConfirmed`    | `inventory.events`     |     P     |             -             |    -    |              -              |                                             |
| `HoldExpired`           | `inventory.events`     |     P     |             C             |    -    |              -              |                                             |
| `WaitlistOffered`       | `inventory.events`     |     P     |             -             |    -    |              C              | `waitlist_offer`                            |
| `OrderCreated`          | `orders.events`        |     -     |             P             |    -    | C (records hold owner only) |                                             |
| `OrderConfirmed`        | `orders.events`        |     C     |             P             |    -    |              C              | `order_confirmed`                           |
| `OrderFailed`           | `orders.events`        |     C     |             P             |    -    |              C              | `order_failed`                              |
| `OrderExpired`          | `orders.events`        |     C     |             P             |    -    |              -              |                                             |
| `PaymentRequested`      | `payments.events`      |     -     |             P             |    C    |              -              |                                             |
| `PaymentSucceeded`      | `payments.events`      |     C     |             C             |    P    |              C              | `payment_succeeded`                         |
| `PaymentFailed`         | `payments.events`      |     C     |             C             |    P    |              C              | `payment_failed`                            |
| `NotificationRequested` | `notifications.events` |     -     |             -             |    -    |              -              | schema defined; no producer or consumer yet |

Notes on "ignores": the consumer receives the message (shared topic) and drops it without side
effects. That is deliberate: other services may publish types a consumer does not care about without
causing dead letters.

---

## Inventory events

Producer: **inventory-service**, via its transactional outbox relay, to `inventory.events`, keyed by
concert id. Emitted in the same database transaction as the state change that caused them.

### `InventoryReserved`

A hold was created and stock deducted. Emitted only by `POST /reservations` (waitlist offers create
holds too, but they emit `WaitlistOffered` instead; see that event).

| Field            | Type         | Notes                                         |
| ---------------- | ------------ | --------------------------------------------- |
| `holdId`         | uuid         |                                               |
| `eventId`        | uuid         | the concert                                   |
| `userId`         | uuid         |                                               |
| `quantity`       | positive int |                                               |
| `unitPriceCents` | int >= 0     |                                               |
| `expiresAt`      | ISO-8601 UTC | hold deadline (database clock)                |
| `reservationId`  | uuid         | equals `holdId` (one reservation == one hold) |

```json
{
  "holdId": "5e1c...",
  "eventId": "00000000-0000-4000-8000-00000000a002",
  "userId": "9a77...",
  "quantity": 2,
  "unitPriceCents": 8000,
  "expiresAt": "2026-11-02T06:05:00.000Z",
  "reservationId": "5e1c..."
}
```

- **Consumers:** order-service creates the order (`PENDING_PAYMENT`, amount = `quantity * unitPriceCents`,
  `UNIQUE(hold_id)`), then emits `OrderCreated`. notification-service stores the hold owner and sends
  `tickets_reserved`.

### `InventoryReleased`

Stock was given back to the pool.

| Field      | Type                                                       |
| ---------- | ---------------------------------------------------------- |
| `holdId`   | uuid                                                       |
| `eventId`  | uuid (concert)                                             |
| `quantity` | positive int                                               |
| `reason`   | `EXPIRED` \| `PAYMENT_FAILED` \| `RELEASED` \| `CANCELLED` |

- **Consumers:** order-service acts only on `EXPIRED` (expires the order for that hold); other reasons
  are ignored (they are a cascade of its own events). Emitted once per hold, only by the single
  transaction that actually moved the hold out of `ACTIVE`.

### `InventoryConfirmed`

A hold became `CONFIRMED` (payment succeeded).

| Field      | Type         |
| ---------- | ------------ |
| `holdId`   | uuid         |
| `eventId`  | uuid         |
| `orderId`  | uuid         |
| `quantity` | positive int |

- **Consumers:** none today. It exists as the inventory-side fact for audit and future consumers
  (analytics, ticket issuing).

### `HoldExpired`

The expiry sweeper released a hold whose `expires_at` passed. Emitted together with
`InventoryReleased(reason = EXPIRED)`.

| Field      | Type         |
| ---------- | ------------ |
| `holdId`   | uuid         |
| `eventId`  | uuid         |
| `quantity` | positive int |

- **Consumers:** order-service expires the matching `PENDING_PAYMENT` order, if any (no order yet =
  nothing to do), then emits `OrderExpired`.

### `WaitlistOffered`

Released stock was matched to a waiting user, and a priority hold was created for them.

| Field        | Type         |
| ------------ | ------------ |
| `waitlistId` | uuid         |
| `eventId`    | uuid         |
| `userId`     | uuid         |
| `quantity`   | positive int |
| `holdId`     | uuid         |
| `expiresAt`  | ISO-8601 UTC |

- **Consumers:** notification-service sends `waitlist_offer` and records the hold owner. The offer is
  a normal `ACTIVE` hold with the usual TTL; if unpaid it expires and the next person in line gets it.
  Note that offers do **not** emit `InventoryReserved`, so no order is created automatically for an
  offered hold in the current implementation (a known gap, see [trade-offs](tradeoffs.md)).

---

## Payment events

### `PaymentRequested` (command)

Producer: **order-service**, to **`payments.events`**, keyed by order id. Written to order-service's
outbox in the same transaction that sets `orders.payment_id` and records the idempotent HTTP response
for `POST /orders/:id/pay`.

| Field         | Type                              |
| ------------- | --------------------------------- |
| `paymentId`   | uuid (minted by order-service)    |
| `orderId`     | uuid                              |
| `holdId`      | uuid                              |
| `userId`      | uuid                              |
| `amountCents` | int >= 0                          |
| `currency`    | 3-letter ISO code (default `USD`) |

- **Consumer:** payment-service. It inserts a `PENDING` payment (`UNIQUE(order_id)`), waits the
  simulated latency (`PAYMENT_LATENCY_MS`, default 500 ms), then settles it and enqueues the result.
  A second request for the same order never charges twice; it re-announces the existing result.

### `PaymentSucceeded`

Producer: **payment-service**, `payments.events`, keyed by order id.

| Field         | Type                             |
| ------------- | -------------------------------- |
| `paymentId`   | uuid                             |
| `orderId`     | uuid                             |
| `holdId`      | uuid                             |
| `amountCents` | int >= 0                         |
| `providerRef` | non-empty string (`mock_<uuid>`) |

- **Consumers:**
  - order-service: `PENDING_PAYMENT -> CONFIRMED`, emits `OrderConfirmed`. Ignores a mismatching
    `paymentId`; for a terminal (`FAILED`/`EXPIRED`) order it logs "refund required" and ignores;
    an amount mismatch is non-retryable and dead-lettered.
  - inventory-service: confirms the hold (`ACTIVE -> CONFIRMED`), emits `InventoryConfirmed`.
  - notification-service: `payment_succeeded` (recipient found via the stored hold owner).

### `PaymentFailed`

Producer: **payment-service**, `payments.events`, keyed by order id. Reasons come from the mock
(`CARD_DECLINED`, `INSUFFICIENT_FUNDS`, `PROCESSOR_TIMEOUT`, `FRAUD_SUSPECTED`) or `SIMULATED_FAILURE`.

| Field         | Type             |
| ------------- | ---------------- |
| `paymentId`   | uuid             |
| `orderId`     | uuid             |
| `holdId`      | uuid             |
| `amountCents` | int >= 0         |
| `reason`      | non-empty string |

- **Consumers:**
  - order-service: `PENDING_PAYMENT -> FAILED`, emits `OrderFailed`.
  - inventory-service: releases the hold with reason `PAYMENT_FAILED` and returns the stock.
  - notification-service: `payment_failed`.

---

## Order events

Producer: **order-service**, via its outbox relay, to `orders.events`, keyed by order id.

### `OrderCreated`

| Field         | Type           |
| ------------- | -------------- |
| `orderId`     | uuid           |
| `holdId`      | uuid           |
| `eventId`     | uuid (concert) |
| `userId`      | uuid           |
| `quantity`    | positive int   |
| `amountCents` | int >= 0       |

- **Consumers:** notification-service records `(holdId, userId)`; no email. inventory-service ignores it.

### `OrderConfirmed`

| Field         | Type           |
| ------------- | -------------- |
| `orderId`     | uuid           |
| `holdId`      | uuid           |
| `eventId`     | uuid (concert) |
| `userId`      | uuid           |
| `quantity`    | positive int   |
| `amountCents` | int >= 0       |
| `paymentId`   | uuid           |

- **Consumers:** inventory-service confirms the hold (idempotent: already confirmed for the same order
  is a no-op); notification-service sends `order_confirmed`.

### `OrderFailed`

| Field     | Type             |
| --------- | ---------------- |
| `orderId` | uuid             |
| `holdId`  | uuid             |
| `eventId` | uuid (concert)   |
| `reason`  | non-empty string |

- **Consumers:** inventory-service releases the hold with reason `CANCELLED` (usually already released
  by `PaymentFailed`; the second release is a no-op); notification-service sends `order_failed`.

### `OrderExpired`

| Field     | Type           |
| --------- | -------------- |
| `orderId` | uuid           |
| `holdId`  | uuid           |
| `eventId` | uuid (concert) |

- **Consumers:** inventory-service releases with reason `EXPIRED` (the hold is normally already
  `EXPIRED`, so a no-op). notification-service ignores it.

---

## Notification events

### `NotificationRequested`

Defined in `packages/shared` and reserved for a future "any service can request a notification"
flow on `notifications.events`.

| Field            | Type                       |
| ---------------- | -------------------------- |
| `notificationId` | uuid                       |
| `userId`         | uuid                       |
| `channel`        | `EMAIL` \| `SMS` \| `PUSH` |
| `template`       | non-empty string           |
| `data`           | object                     |

- **Producers / consumers:** none yet. Today notification-service derives notifications directly from
  the business events above and never emits events itself.

---

## Kafka message headers

Besides the envelope, each producer attaches headers. They are **not uniform across services** (a
known inconsistency; consumers rely on the envelope body, not on these headers):

| Producer          | Headers                                                                                | Message key |
| ----------------- | -------------------------------------------------------------------------------------- | ----------- |
| inventory-service | `correlationId`, `eventId` (the **concert** id), `type`, `messageId` (the envelope id) | concert id  |
| order-service     | `event-type`, `event-id` (the envelope id), `correlation-id`                           | order id    |
| payment-service   | `eventType`, `eventId` (the envelope id)                                               | order id    |

## DLQ and retry headers

| Service              | Headers added when dead-lettering                                                                             |
| -------------------- | ------------------------------------------------------------------------------------------------------------- |
| order-service        | `x-original-topic`, `x-original-partition`, `x-original-offset`, `x-failed-service`, `x-failed-at`, `x-error` |
| inventory-service    | `dlq-reason`, `dlq-error`, `dlq-consumer`, `dlq-original-topic`, `dlq-attempts`, `dlq-failed-at`              |
| payment-service      | `sourceTopic`, `reason`, `service`, `failedAt`                                                                |
| notification-service | `sourceTopic`, `reason`, `service`, `failedAt`                                                                |

Retry headers on `retry.5s`: order-service uses `x-retry-service`, `x-retry-attempt`, `x-retry-at`,
`x-error`; inventory-service uses `retry-consumer`, `retry-attempt`, `retry-original-topic`,
`retry-not-before`, `retry-last-error`, `retry-first-failed-at`. In both cases the **original key and
value are republished untouched**, so the envelope `eventId` is unchanged and de-duplication still
works. `scripts/dlq-replay.ts` understands all of these dialects.

## Rules for evolving events

1. **Adding an optional field** is safe: old consumers ignore it. Keep `version` at 1.
2. **Removing or renaming a field, or changing its meaning,** needs a new `version` and a period in
   which consumers accept both. The `version` field in the envelope exists for this.
3. **Never reuse an `eventId`** for a different message; it is the dedupe key.
4. Producers validate payloads against the shared schema **before** writing to the outbox, so a bad
   event aborts the producing transaction instead of poisoning every consumer.
5. Consumers must be **idempotent** and **tolerant of reordering across keys** (delivery is
   at-least-once; ordering exists only per partition key).
