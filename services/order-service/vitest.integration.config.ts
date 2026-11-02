import { fileURLToPath } from 'node:url';
import { defineConfig } from 'vitest/config';

export default defineConfig({
  resolve: {
    alias: {
      '@seatlock/shared': fileURLToPath(
        new URL('../../packages/shared/src/index.ts', import.meta.url),
      ),
    },
  },
  test: {
    environment: 'node',
    include: ['test/integration/**/*.test.ts'],
    // Pulling images and booting containers is slow on a cold machine.
    testTimeout: 120_000,
    hookTimeout: 180_000,
    // One container set shared by the whole run; do not parallelise files.
    fileParallelism: false,
  },
});
