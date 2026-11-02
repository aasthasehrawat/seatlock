#!/bin/sh
set -e
# Run migrations when the image starts (idempotent).
if [ -f dist/db/migrate-cli.js ]; then
  node dist/db/migrate-cli.js || node dist/db/migrate.js || true
elif [ -f dist/db/migrate.js ]; then
  node dist/db/migrate.js
elif [ -f src/db/migrate.ts ]; then
  npx tsx src/db/migrate.ts
fi
# Optional seed for inventory only
if [ "$RUN_SEED" = "true" ] && [ -f dist/db/seed.js ]; then
  node dist/db/seed.js || true
fi
exec node dist/index.js
