/**
 * Tiny path-prefix gateway for local/public tunnels.
 * Run with services already up (npm run dev), then:
 *   node deploy/local-gateway.mjs
 * Exposes http://localhost:8080/inventory|orders|payments|notifications/...
 */
import http from 'node:http';
import { request as httpRequest } from 'node:http';

const PORT = Number(process.env.GATEWAY_PORT || 8080);
const ROUTES = [
  { prefix: '/inventory', target: process.env.INVENTORY_URL || 'http://127.0.0.1:3001' },
  { prefix: '/orders', target: process.env.ORDER_URL || 'http://127.0.0.1:3002' },
  { prefix: '/payments', target: process.env.PAYMENT_URL || 'http://127.0.0.1:3003' },
  { prefix: '/notifications', target: process.env.NOTIFICATION_URL || 'http://127.0.0.1:3004' },
];

function cors(res, req) {
  const origin = req.headers.origin || '*';
  res.setHeader('Access-Control-Allow-Origin', origin);
  res.setHeader('Access-Control-Allow-Credentials', 'true');
  res.setHeader('Access-Control-Allow-Headers', req.headers['access-control-request-headers'] || '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET,POST,PUT,PATCH,DELETE,OPTIONS');
}

const server = http.createServer((req, res) => {
  cors(res, req);
  if (req.method === 'OPTIONS') {
    res.writeHead(204);
    res.end();
    return;
  }

  if (req.url === '/health' || req.url === '/') {
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ status: 'ok', gateway: true, routes: ROUTES.map((r) => r.prefix) }));
    return;
  }

  const match = ROUTES.find((r) => req.url === r.prefix || req.url.startsWith(r.prefix + '/'));
  if (!match) {
    res.writeHead(404, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ error: 'NOT_FOUND', message: 'Use /inventory, /orders, /payments, or /notifications' }));
    return;
  }

  const targetUrl = new URL(match.target);
  const incoming = new URL(req.url || '/', 'http://gateway.local');
  const strippedPath = incoming.pathname.slice(match.prefix.length) || '/';
  const headers = { ...req.headers, host: targetUrl.host };
  delete headers['content-length'];

  const proxy = httpRequest(
    {
      protocol: targetUrl.protocol,
      hostname: targetUrl.hostname,
      port: targetUrl.port,
      path: strippedPath + incoming.search,
      method: req.method,
      headers,
    },
    (up) => {
      cors(res, req);
      res.writeHead(up.statusCode || 502, up.headers);
      up.pipe(res);
    },
  );
  proxy.on('error', (err) => {
    cors(res, req);
    res.writeHead(502, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ error: 'BAD_GATEWAY', message: err.message }));
  });
  req.pipe(proxy);
});

server.listen(PORT, '0.0.0.0', () => {
  console.log(`SeatLock gateway on http://0.0.0.0:${PORT}`);
  for (const r of ROUTES) console.log(`  ${r.prefix} -> ${r.target}`);
});
