import type { Logger } from 'pino';
import type { Pool } from 'pg';
import { createPoller, type Poller } from '../lib/poller.js';
import { releaseHold } from '../services/hold-lifecycle.js';
import type { ServiceDeps } from '../services/deps.js';

/**
 * HOLD EXPIRY SWEEPER
 * -------------------
 * Every few seconds, find ACTIVE holds past their deadline and release them with reason EXPIRED
 * (which restores stock, emits InventoryReleased + HoldExpired, and offers to the waitlist).
 *
 * Safe to run on many instances at once and alongside payment/order events:
 *  - The scan below is only a *candidate list*; it takes no locks.
 *  - Each candidate is released in its OWN transaction via releaseHold, whose first statement is
 *    `UPDATE holds ... WHERE id = $1 AND status = 'ACTIVE'`. If another sweeper, a payment
 *    failure, or a confirmation got there first, that UPDATE matches no row and the call is a
 *    no-op, so stock is never restored twice and a confirmed hold is never touched.
 *  - One bad hold does not block the others (errors are caught per hold).
 *
 * Time comparison uses the database clock (NOW()), the same clock that stamped expires_at.
 * The partial index idx_holds_expires (ACTIVE only) keeps this query cheap.
 */
export interface HoldExpiryDeps {
  deps: ServiceDeps;
  /** Used for the candidate query (a plain read; no transaction needed). */
  pool: Pick<Pool, 'query'>;
  batchSize: number;
  intervalMs: number;
  logger: Logger;
}

export interface ExpirySweepResult {
  candidates: number;
  released: number;
}

export async function sweepExpiredHolds(
  d: Pick<HoldExpiryDeps, 'deps' | 'pool' | 'batchSize' | 'logger'>,
): Promise<ExpirySweepResult> {
  const { rows } = await d.pool.query<{ id: string }>(
    `SELECT id FROM holds
      WHERE status = 'ACTIVE' AND expires_at < NOW()
      ORDER BY expires_at
      LIMIT $1`,
    [d.batchSize],
  );

  let released = 0;
  for (const { id } of rows) {
    try {
      const result = await releaseHold(d.deps, id, 'EXPIRED');
      if (result.released) released += 1;
    } catch (err) {
      d.logger.error({ err, holdId: id }, 'failed to expire hold');
    }
  }

  if (rows.length > 0) {
    d.logger.info({ candidates: rows.length, released }, 'hold expiry sweep complete');
  }
  return { candidates: rows.length, released };
}

export function createHoldExpiryWorker(d: HoldExpiryDeps): Poller {
  return createPoller({
    name: 'hold-expiry',
    intervalMs: d.intervalMs,
    logger: d.logger,
    run: async () => {
      const { candidates, released } = await sweepExpiredHolds(d);
      // Drain a backlog quickly, but never hot-loop if every release is failing.
      return candidates >= d.batchSize && released > 0;
    },
  });
}
