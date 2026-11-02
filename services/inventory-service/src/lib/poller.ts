import type { Logger } from 'pino';

export interface PollerOptions {
  name: string;
  intervalMs: number;
  logger: Logger;
  /**
   * One unit of work. Return `true` if there is probably more work waiting
   * (e.g. a full batch) so the next run happens immediately instead of after the interval.
   */
  run: () => Promise<boolean | void>;
}

export interface Poller {
  start(): void;
  /** Stops scheduling and resolves once any in-flight run has finished. */
  stop(): Promise<void>;
}

/**
 * Self-scheduling loop (setTimeout, not setInterval) so runs never overlap
 * even if one run takes longer than the interval. Errors are logged, never thrown,
 * so a transient failure (DB blip, Kafka down) just means "try again next tick".
 */
export function createPoller(opts: PollerOptions): Poller {
  let timer: NodeJS.Timeout | undefined;
  let running = false;
  let inFlight: Promise<void> = Promise.resolve();

  const schedule = (delayMs: number): void => {
    if (!running) return;
    timer = setTimeout(() => {
      inFlight = tick();
    }, delayMs);
  };

  const tick = async (): Promise<void> => {
    let more = false;
    try {
      more = (await opts.run()) === true;
    } catch (err) {
      opts.logger.error({ err, poller: opts.name }, 'poller run failed');
    }
    schedule(more ? 0 : opts.intervalMs);
  };

  return {
    start() {
      if (running) return;
      running = true;
      schedule(0);
    },
    async stop() {
      running = false;
      if (timer) clearTimeout(timer);
      await inFlight;
    },
  };
}
