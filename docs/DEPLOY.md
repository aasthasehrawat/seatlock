# Deploy SeatLock so others can use it

## What is live right now

| Piece | URL |
| ----- | --- |
| Frontend (Vercel) | https://seatlock-seven.vercel.app |
| API gateway (Cloudflare tunnel → your PC) | https://helping-zone-erp-modifications.trycloudflare.com |

Vercel env vars (already set in the Vercel project):

```
VITE_INVENTORY_API_URL=https://helping-zone-erp-modifications.trycloudflare.com/inventory
VITE_ORDER_API_URL=https://helping-zone-erp-modifications.trycloudflare.com/orders
VITE_PAYMENT_API_URL=https://helping-zone-erp-modifications.trycloudflare.com/payments
VITE_NOTIFICATION_API_URL=https://helping-zone-erp-modifications.trycloudflare.com/notifications
```

## Important: keep your laptop stack running

The public API tunnel only works while these stay up on your machine:

1. Docker Desktop + `docker compose up -d` (Postgres, Kafka, Redis)
2. `npm run dev` (or the four services individually)
3. `node deploy/local-gateway.mjs` (port 8080)
4. `.\tools\cloudflared.exe tunnel --url http://127.0.0.1:8080`

If you restart the tunnel, Cloudflare gives a **new** URL. Then update the four Vercel env vars and redeploy:

```powershell
cd C:\Users\aasth\seatlock\apps\web
npx vercel deploy --prod --yes --build-env VITE_INVENTORY_API_URL=https://NEW_URL/inventory --build-env VITE_ORDER_API_URL=https://NEW_URL/orders --build-env VITE_PAYMENT_API_URL=https://NEW_URL/payments --build-env VITE_NOTIFICATION_API_URL=https://NEW_URL/notifications
```

## If the Vercel site asks you to log in

In [Vercel Dashboard](https://vercel.com) → project **seatlock** → **Settings** → **Deployment Protection** → turn **Vercel Authentication** **Off** → save. Then anyone can open the link.

## Always-on hosting (optional, later)

For a demo that works when your laptop is off, run the full stack on a VPS:

```powershell
docker compose -f docker-compose.prod.yml up -d --build
```

Point a domain at that server’s port 8080 (or put HTTPS in front with Caddy), then set the Vercel `VITE_*` URLs to that domain and redeploy.
