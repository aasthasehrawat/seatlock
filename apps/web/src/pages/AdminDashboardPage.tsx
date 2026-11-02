import { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import { Link, useSearchParams } from 'react-router-dom';
import {
  Area,
  AreaChart,
  Bar,
  BarChart,
  CartesianGrid,
  Cell,
  Legend,
  Pie,
  PieChart,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from 'recharts';
import {
  errorMessage,
  formatPrice,
  getInventoryAdminOverview,
  getNotificationAdminOverview,
  getOrderAdminOverview,
  type HoldRow,
  type InventoryAdminOverview,
  type NotificationAdminOverview,
  type Order,
  type OrderAdminOverview,
} from '../api/client';
import Spinner from '../components/Spinner';
import StatusBadge from '../components/StatusBadge';

const REFRESH_MS = 15_000;

// Teal / slate palette, matching the CSS custom properties.
const C = {
  teal: '#0f766e',
  cyan: '#0e7490',
  slate: '#64748b',
  red: '#b91c1c',
  green: '#047857',
  amber: '#b45309',
  grid: '#e2e8f0',
};

const HOLD_COLORS: Record<string, string> = {
  ACTIVE: C.cyan,
  CONFIRMED: C.green,
  EXPIRED: C.slate,
  RELEASED: '#94a3b8',
};

const ORDER_COLORS: Record<string, string> = {
  PENDING_PAYMENT: C.amber,
  CONFIRMED: C.green,
  FAILED: C.red,
  EXPIRED: C.slate,
};

const ORDER_LABELS: Record<string, string> = {
  PENDING_PAYMENT: 'Pending',
  CONFIRMED: 'Confirmed',
  FAILED: 'Failed',
  EXPIRED: 'Expired',
};

const nf = new Intl.NumberFormat();

interface Snapshot {
  inventory: InventoryAdminOverview | null;
  orders: OrderAdminOverview | null;
  notifications: NotificationAdminOverview | null;
  errors: string[];
  refreshedAt: Date;
}

type LoadState =
  | { kind: 'loading' }
  | { kind: 'error'; message: string }
  | { kind: 'ready'; snap: Snapshot };

function hourLabel(iso: string): string {
  return new Date(iso).toLocaleTimeString(undefined, { hour: 'numeric' });
}

function timeLabel(iso: string): string {
  return new Date(iso).toLocaleString(undefined, {
    month: 'short',
    day: 'numeric',
    hour: 'numeric',
    minute: '2-digit',
  });
}

function shortId(id: string): string {
  return id.slice(0, 8);
}

function toPieData(counts: Record<string, number>, labels?: Record<string, string>) {
  return Object.entries(counts)
    .filter(([, value]) => value > 0)
    .map(([key, value]) => ({ key, name: labels?.[key] ?? key.toLowerCase(), value }));
}

const HOLD_LABELS: Record<string, string> = {
  ACTIVE: 'Active',
  CONFIRMED: 'Confirmed',
  EXPIRED: 'Expired',
  RELEASED: 'Released',
};

function HoldStatusBadge({ status }: { status: HoldRow['status'] }) {
  const tone = status === 'CONFIRMED' ? 'ok' : status === 'ACTIVE' ? 'warn' : 'neutral';
  return <span className={`badge badge-${tone}`}>{HOLD_LABELS[status] ?? status}</span>;
}

function Kpi({ label, value, hint }: { label: string; value: string; hint?: string }) {
  return (
    <div className="kpi">
      <span className="kpi-label">{label}</span>
      <strong className="kpi-value">{value}</strong>
      {hint && <span className="kpi-hint">{hint}</span>}
    </div>
  );
}

function ChartPanel({
  title,
  subtitle,
  empty,
  children,
}: {
  title: string;
  subtitle?: string;
  empty?: boolean;
  children: React.ReactNode;
}) {
  return (
    <section className="chart-panel" aria-label={title}>
      <h3>{title}</h3>
      {subtitle && <p className="chart-sub">{subtitle}</p>}
      {empty ? <p className="chart-empty">No data yet.</p> : <div className="chart-body">{children}</div>}
    </section>
  );
}

function DonutChart({
  data,
  colors,
  centerLabel,
}: {
  data: Array<{ key: string; name: string; value: number }>;
  colors: Record<string, string>;
  centerLabel: string;
}) {
  const total = data.reduce((sum, d) => sum + d.value, 0);
  return (
    <ResponsiveContainer width="100%" height={240}>
      <PieChart>
        <Pie
          data={data}
          dataKey="value"
          nameKey="name"
          innerRadius={56}
          outerRadius={86}
          paddingAngle={2}
          stroke="none"
        >
          {data.map((d) => (
            <Cell key={d.key} fill={colors[d.key] ?? C.slate} />
          ))}
        </Pie>
        <Tooltip formatter={(v) => nf.format(Number(v))} />
        <Legend verticalAlign="bottom" iconType="circle" iconSize={8} />
        <text x="50%" y="42%" textAnchor="middle" className="donut-total">
          {nf.format(total)}
        </text>
        <text x="50%" y="42%" dy="1.4em" textAnchor="middle" className="donut-caption">
          {centerLabel}
        </text>
      </PieChart>
    </ResponsiveContainer>
  );
}

function Dashboard({ snap }: { snap: Snapshot }) {
  const { inventory, orders, notifications } = snap;

  const holdsSeries = useMemo(
    () =>
      (inventory?.activity.holdsCreatedByHour ?? []).map((b) => ({
        label: hourLabel(b.hour),
        tickets: b.tickets,
        holds: b.count,
      })),
    [inventory],
  );

  const ordersSeries = useMemo(
    () =>
      (orders?.activity.ordersByHour ?? []).map((b) => ({
        label: hourLabel(b.hour),
        confirmed: b.confirmed,
        failed: b.failed,
      })),
    [orders],
  );

  const holdPie = useMemo(() => toPieData(inventory?.holds.byStatus ?? {}, HOLD_LABELS), [inventory]);
  const orderPie = useMemo(() => toPieData(orders?.orders.byStatus ?? {}, ORDER_LABELS), [orders]);
  const templateData = useMemo(
    () =>
      Object.entries(notifications?.byTemplate ?? {})
        .sort((a, b) => b[1] - a[1])
        .map(([template, count]) => ({ template, count })),
    [notifications],
  );

  const eventNames = useMemo(() => {
    const map = new Map<string, string>();
    for (const e of inventory?.events ?? []) map.set(e.id, e.name);
    return map;
  }, [inventory]);

  const confirmedOrders = orders?.orders.byStatus.CONFIRMED ?? 0;

  return (
    <>
      {snap.errors.length > 0 && (
        <div className="notice notice-error" role="alert">
          <div>
            <strong>Some data could not be loaded.</strong>
            {snap.errors.map((e) => (
              <p key={e}>{e}</p>
            ))}
          </div>
        </div>
      )}

      <div className="kpi-grid">
        <Kpi
          label="Events"
          value={inventory ? nf.format(inventory.inventory.eventCount) : '-'}
          hint={inventory ? `${nf.format(inventory.inventory.totalTickets)} total tickets` : undefined}
        />
        <Kpi
          label="Tickets available"
          value={inventory ? nf.format(inventory.inventory.availableTickets) : '-'}
          hint={inventory ? `${nf.format(inventory.inventory.reservedTickets)} held or sold` : undefined}
        />
        <Kpi
          label="Sold"
          value={inventory ? `${inventory.inventory.soldPct.toFixed(1)}%` : '-'}
          hint="of all inventory reserved"
        />
        <Kpi
          label="Confirmed revenue"
          value={orders ? formatPrice(orders.orders.totalRevenueCents) : '-'}
          hint={orders ? `${nf.format(confirmedOrders)} confirmed orders` : undefined}
        />
        <Kpi
          label="Waitlist waiting"
          value={inventory ? nf.format(inventory.waitlist.waitingCount) : '-'}
          hint="people queued"
        />
      </div>

      <div className="chart-grid">
        <ChartPanel
          title="Holds created, last 24 hours"
          subtitle="Tickets held per hour"
          empty={!inventory || holdsSeries.every((p) => p.tickets === 0)}
        >
          <ResponsiveContainer width="100%" height={240}>
            <AreaChart data={holdsSeries} margin={{ top: 8, right: 8, left: -12, bottom: 0 }}>
              <defs>
                <linearGradient id="holdsFill" x1="0" y1="0" x2="0" y2="1">
                  <stop offset="0%" stopColor={C.teal} stopOpacity={0.35} />
                  <stop offset="100%" stopColor={C.teal} stopOpacity={0.02} />
                </linearGradient>
              </defs>
              <CartesianGrid stroke={C.grid} strokeDasharray="3 3" vertical={false} />
              <XAxis dataKey="label" tick={{ fill: C.slate, fontSize: 12 }} interval="preserveStartEnd" minTickGap={24} />
              <YAxis tick={{ fill: C.slate, fontSize: 12 }} allowDecimals={false} />
              <Tooltip />
              <Area
                type="monotone"
                dataKey="tickets"
                name="Tickets held"
                stroke={C.teal}
                strokeWidth={2}
                fill="url(#holdsFill)"
              />
              <Area
                type="monotone"
                dataKey="holds"
                name="Holds"
                stroke={C.cyan}
                strokeWidth={1.5}
                strokeDasharray="4 3"
                fill="none"
              />
            </AreaChart>
          </ResponsiveContainer>
        </ChartPanel>

        <ChartPanel
          title="Orders by hour"
          subtitle="Confirmed vs failed, last 24 hours"
          empty={!orders || ordersSeries.every((p) => p.confirmed === 0 && p.failed === 0)}
        >
          <ResponsiveContainer width="100%" height={240}>
            <BarChart data={ordersSeries} margin={{ top: 8, right: 8, left: -12, bottom: 0 }}>
              <CartesianGrid stroke={C.grid} strokeDasharray="3 3" vertical={false} />
              <XAxis dataKey="label" tick={{ fill: C.slate, fontSize: 12 }} interval="preserveStartEnd" minTickGap={24} />
              <YAxis tick={{ fill: C.slate, fontSize: 12 }} allowDecimals={false} />
              <Tooltip cursor={{ fill: 'rgba(100,116,139,0.1)' }} />
              <Legend iconType="circle" iconSize={8} />
              <Bar dataKey="confirmed" name="Confirmed" stackId="o" fill={C.green} />
              <Bar dataKey="failed" name="Failed" stackId="o" fill={C.red} radius={[3, 3, 0, 0]} />
            </BarChart>
          </ResponsiveContainer>
        </ChartPanel>

        <ChartPanel title="Hold status" subtitle="All holds" empty={!inventory || holdPie.length === 0}>
          <DonutChart
            data={holdPie}
            colors={HOLD_COLORS}
            centerLabel="holds"
          />
        </ChartPanel>

        <ChartPanel title="Order status" subtitle="All orders" empty={!orders || orderPie.length === 0}>
          <DonutChart
            data={orderPie}
            colors={ORDER_COLORS}
            centerLabel="orders"
          />
        </ChartPanel>
      </div>

      <section className="admin-section" aria-labelledby="inv-heading">
        <div className="section-head">
          <h2 id="inv-heading" className="section-title">
            Inventory by event
          </h2>
          {inventory && <span className="muted small">{inventory.events.length} events</span>}
        </div>
        {inventory && inventory.events.length === 0 ? (
          <p className="empty">
            No events yet. <Link to="/admin/events/new">Create one</Link>, or run <code>npm run seed:demo</code>.
          </p>
        ) : (
          inventory && (
            <div className="table-wrap">
              <table className="data-table">
                <thead>
                  <tr>
                    <th>Event</th>
                    <th>Starts</th>
                    <th className="col-fill">Available / total</th>
                    <th className="num">Held</th>
                    <th className="num">Waitlist</th>
                    <th className="num">Sold est.</th>
                    <th className="num">Price</th>
                    <th />
                  </tr>
                </thead>
                <tbody>
                  {inventory.events.map((e) => {
                    const pct = e.totalTickets > 0 ? (e.available / e.totalTickets) * 100 : 0;
                    return (
                      <tr key={e.id}>
                        <td>
                          <strong>{e.name}</strong>
                          {e.venue && <div className="muted small">{e.venue}</div>}
                        </td>
                        <td className="nowrap">{timeLabel(e.startsAt)}</td>
                        <td className="col-fill">
                          <div
                            className="progress-track"
                            role="progressbar"
                            aria-valuemin={0}
                            aria-valuemax={e.totalTickets}
                            aria-valuenow={e.available}
                            aria-label={`${e.name}: ${e.available} of ${e.totalTickets} available`}
                          >
                            <div className="progress-fill" style={{ width: `${Math.min(100, pct)}%` }} />
                          </div>
                          <span className="small muted">
                            {nf.format(e.available)} / {nf.format(e.totalTickets)}
                          </span>
                        </td>
                        <td className="num">{nf.format(e.heldActive)}</td>
                        <td className="num">{nf.format(e.waitlistWaiting)}</td>
                        <td className="num">{nf.format(e.soldEstimate)}</td>
                        <td className="num">{formatPrice(e.unitPriceCents)}</td>
                        <td className="num">
                          <Link className="row-link" to={`/events/${e.id}`}>
                            View
                          </Link>
                        </td>
                      </tr>
                    );
                  })}
                </tbody>
              </table>
            </div>
          )
        )}
      </section>

      <div className="table-grid">
        <section className="admin-section" aria-labelledby="holds-heading">
          <div className="section-head">
            <h2 id="holds-heading" className="section-title">
              Recent holds
            </h2>
          </div>
          {inventory && inventory.holds.recent.length === 0 && <p className="empty">No holds yet.</p>}
          {inventory && inventory.holds.recent.length > 0 && (
            <div className="table-wrap">
              <table className="data-table">
                <thead>
                  <tr>
                    <th>Event</th>
                    <th className="num">Qty</th>
                    <th>Status</th>
                    <th>Created</th>
                  </tr>
                </thead>
                <tbody>
                  {inventory.holds.recent.map((h) => (
                    <tr key={h.id}>
                      <td>
                        {h.eventName ?? eventNames.get(h.eventId) ?? shortId(h.eventId)}
                        <div className="mono muted small">{shortId(h.id)}</div>
                      </td>
                      <td className="num">{h.quantity}</td>
                      <td>
                        <HoldStatusBadge status={h.status} />
                      </td>
                      <td className="nowrap">{timeLabel(h.createdAt)}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )}
        </section>

        <section className="admin-section" aria-labelledby="orders-heading">
          <div className="section-head">
            <h2 id="orders-heading" className="section-title">
              Recent orders
            </h2>
          </div>
          {orders && orders.orders.recent.length === 0 && <p className="empty">No orders yet.</p>}
          {orders && orders.orders.recent.length > 0 && (
            <div className="table-wrap">
              <table className="data-table">
                <thead>
                  <tr>
                    <th>Order</th>
                    <th className="num">Qty</th>
                    <th className="num">Amount</th>
                    <th>Status</th>
                    <th>Created</th>
                  </tr>
                </thead>
                <tbody>
                  {orders.orders.recent.map((o: Order) => (
                    <tr key={o.id}>
                      <td>
                        <Link className="row-link mono" to={`/orders/${o.id}`}>
                          {shortId(o.id)}
                        </Link>
                        <div className="muted small">{eventNames.get(o.eventId) ?? shortId(o.eventId)}</div>
                      </td>
                      <td className="num">{o.quantity}</td>
                      <td className="num">{formatPrice(o.amountCents)}</td>
                      <td>
                        <StatusBadge status={o.status} />
                      </td>
                      <td className="nowrap">{timeLabel(o.createdAt)}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )}
        </section>
      </div>

      <section className="admin-section" aria-labelledby="notif-heading">
        <div className="section-head">
          <h2 id="notif-heading" className="section-title">
            Notifications
          </h2>
          {notifications && (
            <span className="muted small">
              {nf.format(notifications.total)} total
              {' \u00b7 '}
              {nf.format(notifications.byStatus.SENT ?? 0)} sent
              {' \u00b7 '}
              {nf.format(notifications.byStatus.FAILED ?? 0)} failed
            </span>
          )}
        </div>
        {notifications && templateData.length === 0 && <p className="empty">No notifications yet.</p>}
        {notifications && templateData.length > 0 && (
          <div className="notif-strip">
            <div className="notif-chart" role="img" aria-label="Notifications by template">
              <ResponsiveContainer width="100%" height={Math.max(120, templateData.length * 34 + 20)}>
                <BarChart data={templateData} layout="vertical" margin={{ top: 0, right: 16, left: 8, bottom: 0 }}>
                  <CartesianGrid stroke={C.grid} strokeDasharray="3 3" horizontal={false} />
                  <XAxis type="number" allowDecimals={false} tick={{ fill: C.slate, fontSize: 12 }} />
                  <YAxis
                    type="category"
                    dataKey="template"
                    width={150}
                    tick={{ fill: C.slate, fontSize: 12 }}
                  />
                  <Tooltip cursor={{ fill: 'rgba(100,116,139,0.1)' }} />
                  <Bar dataKey="count" name="Notifications" fill={C.cyan} radius={[0, 3, 3, 0]} />
                </BarChart>
              </ResponsiveContainer>
            </div>
            <ul className="notif-list">
              {notifications.recent.slice(0, 6).map((n) => (
                <li key={n.id}>
                  <span className="mono">{n.template}</span>
                  <span className="muted small">{timeLabel(n.createdAt)}</span>
                  <span className={`badge badge-${n.status === 'SENT' ? 'ok' : n.status === 'FAILED' ? 'bad' : 'warn'}`}>
                    {n.status.toLowerCase()}
                  </span>
                </li>
              ))}
            </ul>
          </div>
        )}
      </section>
    </>
  );
}

export default function AdminDashboardPage() {
  const [searchParams, setSearchParams] = useSearchParams();
  const createdName = searchParams.get('name');
  const createdId = searchParams.get('created');
  const [state, setState] = useState<LoadState>({ kind: 'loading' });
  const [refreshing, setRefreshing] = useState(false);
  const [auto, setAuto] = useState(true);
  const controllerRef = useRef<AbortController | null>(null);

  const load = useCallback(async () => {
    controllerRef.current?.abort();
    const controller = new AbortController();
    controllerRef.current = controller;
    setRefreshing(true);

    const [inv, ord, notif] = await Promise.allSettled([
      getInventoryAdminOverview(controller.signal),
      getOrderAdminOverview(controller.signal),
      getNotificationAdminOverview(controller.signal),
    ]);
    if (controller.signal.aborted) return;

    const errors: string[] = [];
    const pick = <T,>(r: PromiseSettledResult<T>, name: string): T | null => {
      if (r.status === 'fulfilled') return r.value;
      errors.push(`${name}: ${errorMessage(r.reason)}`);
      return null;
    };
    const inventory = pick(inv, 'Inventory service');
    const orders = pick(ord, 'Order service');
    const notifications = pick(notif, 'Notification service');

    setRefreshing(false);
    if (!inventory && !orders && !notifications) {
      setState({ kind: 'error', message: errors.join(' ') });
      return;
    }
    setState({
      kind: 'ready',
      snap: { inventory, orders, notifications, errors, refreshedAt: new Date() },
    });
  }, []);

  useEffect(() => {
    void load();
    return () => controllerRef.current?.abort();
  }, [load]);

  useEffect(() => {
    if (!auto) return;
    const id = window.setInterval(() => {
      if (document.visibilityState === 'visible') void load();
    }, REFRESH_MS);
    return () => window.clearInterval(id);
  }, [auto, load]);

  return (
    <div className="page admin-page">
      <div className="admin-toolbar">
        <div>
          <p className="eyebrow-dark">Admin</p>
          <h1 className="page-title">Operations</h1>
          <p className="muted small" aria-live="polite">
            {state.kind === 'ready'
              ? `Last refreshed ${state.snap.refreshedAt.toLocaleTimeString()}`
              : 'Waiting for first load'}
            {auto ? ` \u00b7 auto-refresh every ${REFRESH_MS / 1000}s` : ''}
          </p>
        </div>
        <div className="admin-actions">
          <label className="toggle">
            <input type="checkbox" checked={auto} onChange={(e) => setAuto(e.target.checked)} />
            <span>Auto-refresh</span>
          </label>
          <button type="button" className="btn btn-secondary" onClick={() => void load()} disabled={refreshing}>
            {refreshing ? 'Refreshing...' : 'Refresh'}
          </button>
          <Link to="/admin/events/new" className="btn btn-primary">
            Create event
          </Link>
        </div>
      </div>

      {state.kind === 'loading' && <Spinner label="Loading dashboard..." />}

      {createdId && (
        <div className="notice notice-ok" role="status">
          <div>
            <strong>Event saved to Postgres.</strong>
            <p>
              {createdName ? (
                <>
                  <em>{createdName}</em> is in the inventory <code>events</code> table
                </>
              ) : (
                <>New event is in the inventory <code>events</code> table</>
              )}{' '}
              (id <span className="mono">{createdId.slice(0, 8)}…</span>). It also appears on the public
              Events page.
            </p>
          </div>
          <div className="notice-actions">
            <Link className="btn btn-secondary" to={`/events/${createdId}`}>
              View event
            </Link>
            <button
              type="button"
              className="btn btn-secondary"
              onClick={() => setSearchParams({}, { replace: true })}
            >
              Dismiss
            </button>
          </div>
        </div>
      )}

      {state.kind === 'error' && (
        <div className="notice notice-error" role="alert">
          <div>
            <strong>Couldn&apos;t load the dashboard.</strong>
            <p>{state.message}</p>
            <p className="small">
              The charts read live data from Postgres. Start Docker Desktop, then in the project folder run{' '}
              <code>docker compose up -d</code> and <code>npm run dev</code>, wait ~20 seconds, and try
              again.
            </p>
          </div>
          <button type="button" className="btn btn-secondary" onClick={() => void load()}>
            Try again
          </button>
        </div>
      )}

      {state.kind === 'ready' && <Dashboard snap={state.snap} />}
    </div>
  );
}
