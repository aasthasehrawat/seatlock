import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { config as loadDotenv } from 'dotenv';

/**
 * Side-effect module: load `.env` files before configuration is read.
 * dotenv never overrides variables that are already set, so precedence is:
 * real environment > service-level .env > repo-root .env.
 *
 * Works from both `src/lib` (tsx) and `dist/lib` (compiled): the service root is two levels up.
 */
const here = path.dirname(fileURLToPath(import.meta.url));
loadDotenv({ path: path.resolve(here, '../../.env') });
loadDotenv({ path: path.resolve(here, '../../../../.env') });
