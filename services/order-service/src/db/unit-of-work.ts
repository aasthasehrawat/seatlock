import type { Pool } from 'pg';
import { PgIdempotencyRepo } from './idempotency-repo.js';
import { PgOrdersRepo } from './orders-repo.js';
import { PgOutboxRepo } from './outbox-repo.js';
import { toQueryable } from './pool.js';
import { PgProcessedEventsRepo } from './processed-events-repo.js';
import type { Queryable, TxContext, UnitOfWork } from './types.js';

function createContext(db: Queryable): TxContext {
  return {
    orders: new PgOrdersRepo(db),
    outbox: new PgOutboxRepo(db),
    idempotency: new PgIdempotencyRepo(db),
    processedEvents: new PgProcessedEventsRepo(db),
  };
}

/**
 * Postgres-backed unit of work. Every repository inside `run` shares ONE connection and ONE
 * transaction, which is what makes "state change + outbox row" atomic.
 *
 * Isolation is READ COMMITTED (the PG default). Correctness under concurrency comes from
 * explicit row locks (`SELECT ... FOR UPDATE`) and advisory locks, not from SERIALIZABLE retries.
 */
export class PgUnitOfWork implements UnitOfWork {
  constructor(private readonly pool: Pool) {}

  async run<T>(fn: (ctx: TxContext) => Promise<T>): Promise<T> {
    const client = await this.pool.connect();
    let broken = false;

    try {
      await client.query('BEGIN');
      const result = await fn(createContext(toQueryable(client)));
      await client.query('COMMIT');
      return result;
    } catch (err) {
      try {
        await client.query('ROLLBACK');
      } catch {
        // The connection is unusable; make sure the pool discards it instead of reusing it.
        broken = true;
      }
      throw err;
    } finally {
      client.release(broken ? true : undefined);
    }
  }

  async read<T>(fn: (ctx: TxContext) => Promise<T>): Promise<T> {
    return fn(createContext(toQueryable(this.pool)));
  }
}
