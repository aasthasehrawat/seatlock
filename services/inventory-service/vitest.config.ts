import { defineConfig } from 'vitest/config';

export default defineConfig({
  test: {
    environment: 'node',
    include: ['src/**/*.test.ts'],
    // Integration tests start Docker containers, so give them generous headroom.
    testTimeout: 60_000,
    hookTimeout: 180_000,
  },
});
