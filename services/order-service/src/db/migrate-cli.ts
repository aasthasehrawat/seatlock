import '../lib/load-env.js';
import { loadConfig } from '../config.js';
import { createLogger } from '../lib/logger.js';
import { runMigrations } from './migrate.js';
import { createPool } from './pool.js';

/** `npm run migrate` entry point. */
async function main(): Promise<void> {
  const config = loadConfig();
  const logger = createLogger(config);
  const pool = createPool(config, logger);

  try {
    await runMigrations(pool, logger);
  } finally {
    await pool.end();
  }
}

main().catch((err: unknown) => {
  // eslint-disable-next-line no-console
  console.error('migration failed', err);
  process.exit(1);
});
