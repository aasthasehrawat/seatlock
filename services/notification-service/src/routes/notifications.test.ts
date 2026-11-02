import { afterEach, describe, expect, it } from 'vitest';
import { createId } from '@seatlock/shared';
import { buildApp } from '../app.js';
import { createLogger } from '../lib/logger.js';
import type { NewNotification } from '../notifications/types.js';
import { InMemoryNotificationRepository } from '../test-support/in-memory-repository.js';

const logger = createLogger({ level: 'silent', name: 'test' });

let app: Awaited<ReturnType<typeof buildApp>> | undefined;

async function setup(ready = true) {
  const repo = new InMemoryNotificationRepository();
  app = await buildApp({ logger, repo, readinessChecks: { database: async () => ready } });
  return { app, repo };
}

async function seed(repo: InMemoryNotificationRepository, userId: string, template = 'order_confirmed') {
  const notification: NewNotification = {
    id: createId(),
    userId,
    channel: 'EMAIL',
    template,
    payload: { hello: 'world' },
    status: 'SENT',
  };
  await repo.withTransaction((tx) => tx.insertNotification(notification));
  return notification;
}

afterEach(async () => {
  await app?.close();
  app = undefined;
});

describe('health routes', () => {
  it('reports live', async () => {
    const { app } = await setup();
    const res = await app.inject({ method: 'GET', url: '/health/live' });
    expect(res.statusCode).toBe(200);
    expect(res.json()).toEqual({ status: 'ok' });
  });

  it('reports ready and not ready', async () => {
    const ready = await setup(true);
    expect((await ready.app.inject({ method: 'GET', url: '/health/ready' })).statusCode).toBe(200);
    await app?.close();

    const notReady = await setup(false);
    const res = await notReady.app.inject({ method: 'GET', url: '/health/ready' });
    expect(res.statusCode).toBe(503);
    expect(res.json()).toMatchObject({ status: 'not_ready' });
  });
});

describe('notification routes', () => {
  it('lists notifications for a user, newest first', async () => {
    const { app, repo } = await setup();
    const userId = createId();
    const first = await seed(repo, userId, 'tickets_reserved');
    const second = await seed(repo, userId, 'order_confirmed');
    await seed(repo, createId());

    const res = await app.inject({ method: 'GET', url: `/notifications?userId=${userId}` });
    expect(res.statusCode).toBe(200);
    const body = res.json();
    expect(body.notifications.map((n: { id: string }) => n.id)).toEqual([second.id, first.id]);
    expect(body.notifications[0]).toMatchObject({ userId, channel: 'EMAIL', status: 'SENT' });
  });

  it('requires a valid userId', async () => {
    const { app } = await setup();
    expect((await app.inject({ method: 'GET', url: '/notifications' })).statusCode).toBe(400);
    expect((await app.inject({ method: 'GET', url: '/notifications?userId=abc' })).statusCode).toBe(400);
  });

  it('gets a notification by id', async () => {
    const { app, repo } = await setup();
    const seeded = await seed(repo, createId());

    const res = await app.inject({ method: 'GET', url: `/notifications/${seeded.id}` });
    expect(res.statusCode).toBe(200);
    expect(res.json()).toMatchObject({ id: seeded.id, template: 'order_confirmed', payload: { hello: 'world' } });
  });

  it('returns 404 for unknown ids and 400 for malformed ids', async () => {
    const { app } = await setup();
    const missing = await app.inject({ method: 'GET', url: `/notifications/${createId()}` });
    expect(missing.statusCode).toBe(404);
    expect(missing.json()).toMatchObject({ error: 'NOT_FOUND' });

    expect((await app.inject({ method: 'GET', url: '/notifications/not-a-uuid' })).statusCode).toBe(400);
  });
});
