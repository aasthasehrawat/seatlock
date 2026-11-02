import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { config as loadDotenv } from 'dotenv';

/**
 * Load `.env` before configuration is read.
 * Precedence: real environment > service-level .env > repo-root .env.
 */
const here = path.dirname(fileURLToPath(import.meta.url));
loadDotenv({ path: path.resolve(here, '../../.env') });
loadDotenv({ path: path.resolve(here, '../../../../.env') });
