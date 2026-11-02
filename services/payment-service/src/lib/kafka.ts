import { Kafka, logLevel, type Producer } from 'kafkajs';
import { EventEnvelopeSchema, TOPICS, type EventEnvelope } from '@seatlock/shared';

export interface KafkaOptions {
  brokers: string[];
  clientId: string;
}

export function createKafka(options: KafkaOptions): Kafka {
  return new Kafka({
    clientId: options.clientId,
    brokers: options.brokers,
    logLevel: logLevel.WARN,
    retry: { initialRetryTime: 300, retries: 8 },
  });
}

export function createProducer(kafka: Kafka): Producer {
  return kafka.producer({ idempotent: true, maxInFlightRequests: 1, allowAutoTopicCreation: true });
}

export type DecodeResult =
  | { ok: true; envelope: EventEnvelope }
  | { ok: false; reason: string };

/** Decode a raw Kafka message value into a validated event envelope. */
export function decodeEnvelope(value: Buffer | string | null | undefined): DecodeResult {
  if (value === null || value === undefined) {
    return { ok: false, reason: 'Empty message value' };
  }

  let json: unknown;
  try {
    json = JSON.parse(value.toString());
  } catch {
    return { ok: false, reason: 'Message is not valid JSON' };
  }

  const parsed = EventEnvelopeSchema.safeParse(json);
  if (!parsed.success) {
    return {
      ok: false,
      reason: `Invalid envelope: ${parsed.error.issues.map((i) => `${i.path.join('.')} ${i.message}`).join('; ')}`,
    };
  }
  // z.unknown() infers an optional key; EventEnvelope requires `payload` to be present.
  return { ok: true, envelope: { ...parsed.data, payload: parsed.data.payload } };
}

export interface DlqMessage {
  sourceTopic: string;
  key: Buffer | string | null | undefined;
  value: Buffer | string | null | undefined;
  reason: string;
  service: string;
}

/** Park a poison message on the dead-letter topic with enough context to replay it. */
export async function sendToDlq(producer: Producer, message: DlqMessage): Promise<void> {
  await producer.send({
    topic: TOPICS.DLQ,
    messages: [
      {
        key: message.key ?? null,
        value: message.value ?? null,
        headers: {
          sourceTopic: message.sourceTopic,
          reason: message.reason,
          service: message.service,
          failedAt: new Date().toISOString(),
        },
      },
    ],
  });
}
