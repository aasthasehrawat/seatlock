/** Kafka topic names. Retry topics delay redelivery; DLQ holds poison messages. */
export const TOPICS = {
  INVENTORY: 'inventory.events',
  ORDERS: 'orders.events',
  PAYMENTS: 'payments.events',
  NOTIFICATIONS: 'notifications.events',
  RETRY_5S: 'retry.5s',
  RETRY_30S: 'retry.30s',
  DLQ: 'dlq',
} as const;

export type Topic = (typeof TOPICS)[keyof typeof TOPICS];
