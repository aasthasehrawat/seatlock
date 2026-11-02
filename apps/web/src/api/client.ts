/**
 * Thin typed wrapper around the SeatLock backend services.
 *
 *   inventory-service  -> events, reservations, waitlist
 *   order-service      -> orders, payment initiation
 *   payment-service    -> (processes payments asynchronously; the UI never calls it directly)
 */

export const INVENTORY_API_URL = import.meta.env.VITE_INVENTORY_API_URL ?? 'http://localhost:3001';
export const ORDER_API_URL = import.meta.env.VITE_ORDER_API_URL ?? 'http://localhost:3002';
export const PAYMENT_API_URL = import.meta.env.VITE_PAYMENT_API_URL ?? 'http://localhost:3003';
export const NOTIFICATION_API_URL =
  import.meta.env.VITE_NOTIFICATION_API_URL ?? 'http://localhost:3004';

// ---------------------------------------------------------------------------
// Types (mirror the backend DTOs)
// ---------------------------------------------------------------------------

export interface EventSummary {
  id: string;
  name: string;
  description: string | null;
  venue: string | null;
  startsAt: string;
  totalTickets: number;
  available: number;
  unitPriceCents: number;
}

export interface Reservation {
  holdId: string;
  eventId: string;
  userId: string;
  quantity: number;
  status: 'ACTIVE';
  expiresAt: string;
  unitPriceCents: number;
  totalPriceCents: number;
}

export interface WaitlistEntry {
  waitlistId: string;
  eventId: string;
  userId: string;
  quantity: number;
  status: 'WAITING';
  position: number;
}

export type OrderStatus = 'PENDING_PAYMENT' | 'CONFIRMED' | 'FAILED' | 'EXPIRED';

export interface Order {
  id: string;
  holdId: string;
  eventId: string;
  userId: string;
  quantity: number;
  amountCents: number;
  status: OrderStatus;
  paymentId: string | null;
  createdAt: string;
  updatedAt: string;
}

export interface PaymentInitiation {
  orderId: string;
  paymentId: string;
  status: OrderStatus;
  paymentStatus: 'REQUESTED';
  amountCents: number;
  currency: string;
}

// --- Admin / dashboard DTOs (demo-only, unauthenticated endpoints) ---

export type HoldStatus = 'ACTIVE' | 'CONFIRMED' | 'EXPIRED' | 'RELEASED';

export interface HoldRow {
  id: string;
  eventId: string;
  eventName?: string;
  userId: string;
  quantity: number;
  status: HoldStatus;
  expiresAt: string;
  createdAt: string;
  orderId?: string | null;
}

export interface InventoryAdminEventRow {
  id: string;
  name: string;
  venue: string | null;
  startsAt: string;
  totalTickets: number;
  available: number;
  unitPriceCents: number;
  heldActive: number;
  waitlistWaiting: number;
  soldEstimate: number;
}

export interface InventoryAdminOverview {
  generatedAt: string;
  inventory: {
    eventCount: number;
    totalTickets: number;
    availableTickets: number;
    reservedTickets: number;
    soldPct: number;
  };
  holds: { byStatus: Record<string, number>; recent: HoldRow[] };
  waitlist: { byStatus: Record<string, number>; waitingCount: number };
  events: InventoryAdminEventRow[];
  activity: { holdsCreatedByHour: Array<{ hour: string; count: number; tickets: number }> };
}

export interface OrderAdminOverview {
  generatedAt: string;
  orders: {
    byStatus: Record<string, number>;
    totalRevenueCents: number;
    recent: Order[];
  };
  activity: {
    ordersByHour: Array<{
      hour: string;
      count: number;
      confirmed: number;
      failed: number;
      revenueCents: number;
    }>;
  };
}

export interface NotificationRow {
  id: string;
  userId: string;
  channel: string;
  template: string;
  payload: Record<string, unknown>;
  status: 'PENDING' | 'SENT' | 'FAILED';
  createdAt: string;
}

export interface NotificationAdminOverview {
  generatedAt: string;
  total: number;
  byStatus: Record<string, number>;
  byTemplate: Record<string, number>;
  recent: NotificationRow[];
}

export interface CreateEventInput {
  name: string;
  description?: string;
  venue?: string;
  /** ISO-8601 with offset, e.g. 2026-12-01T19:30:00.000Z */
  startsAt: string;
  totalTickets: number;
  unitPriceCents: number;
}

export const TERMINAL_ORDER_STATUSES: readonly OrderStatus[] = ['CONFIRMED', 'FAILED', 'EXPIRED'];

export function isTerminal(status: OrderStatus): boolean {
  return TERMINAL_ORDER_STATUSES.includes(status);
}

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

export class ApiError extends Error {
  readonly status: number;
  readonly code: string;

  constructor(status: number, code: string, message: string) {
    super(message);
    this.name = 'ApiError';
    this.status = status;
    this.code = code;
  }

  get isSoldOut(): boolean {
    return this.status === 409 && this.code === 'SOLD_OUT';
  }
}

/** Turn any thrown value into a message that is safe to show to a user. */
export function errorMessage(err: unknown): string {
  if (err instanceof ApiError) {
    if (err.isSoldOut) return 'Sold out. Someone grabbed those tickets a moment ago.';
    if (err.status === 429) return 'Too many attempts. Please wait a few seconds and try again.';
    return err.message;
  }
  if (err instanceof Error) return err.message;
  return 'Something went wrong. Please try again.';
}

// ---------------------------------------------------------------------------
// Identity + helpers
// ---------------------------------------------------------------------------

const USER_ID_KEY = 'seatlock.userId';

/** Fallback for non-secure contexts where crypto.randomUUID is unavailable. */
export function newUuid(): string {
  if (typeof crypto !== 'undefined' && typeof crypto.randomUUID === 'function') {
    return crypto.randomUUID();
  }
  return 'xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx'.replace(/[xy]/g, (c) => {
    const r = (Math.random() * 16) | 0;
    return (c === 'x' ? r : (r & 0x3) | 0x8).toString(16);
  });
}

/** Anonymous per-browser identity: created once, then reused forever. */
export function getUserId(): string {
  try {
    const existing = localStorage.getItem(USER_ID_KEY);
    if (existing) return existing;
    const created = newUuid();
    localStorage.setItem(USER_ID_KEY, created);
    return created;
  } catch {
    // localStorage blocked (private mode etc.): fall back to a per-tab id.
    return newUuid();
  }
}

export function formatPrice(cents: number): string {
  return new Intl.NumberFormat(undefined, { style: 'currency', currency: 'USD' }).format(cents / 100);
}

// ---------------------------------------------------------------------------
// Core request function
// ---------------------------------------------------------------------------

interface RequestOptions {
  method?: 'GET' | 'POST';
  body?: unknown;
  idempotencyKey?: string;
  signal?: AbortSignal;
}

async function request<T>(baseUrl: string, path: string, opts: RequestOptions = {}): Promise<T> {
  const headers: Record<string, string> = { Accept: 'application/json' };
  if (opts.body !== undefined) headers['Content-Type'] = 'application/json';
  if (opts.idempotencyKey) headers['Idempotency-Key'] = opts.idempotencyKey;

  let res: Response;
  try {
    res = await fetch(`${baseUrl}${path}`, {
      method: opts.method ?? 'GET',
      headers,
      body: opts.body !== undefined ? JSON.stringify(opts.body) : undefined,
      signal: opts.signal,
    });
  } catch (err) {
    if (err instanceof DOMException && err.name === 'AbortError') throw err;
    throw new ApiError(0, 'NETWORK_ERROR', 'Cannot reach the server. Check your connection and try again.');
  }

  const text = await res.text();
  let data: unknown = undefined;
  if (text) {
    try {
      data = JSON.parse(text);
    } catch {
      data = undefined;
    }
  }

  if (!res.ok) {
    const err = (data as { error?: { code?: string; message?: string } } | undefined)?.error;
    throw new ApiError(
      res.status,
      err?.code ?? 'HTTP_ERROR',
      err?.message ?? `Request failed (${res.status})`,
    );
  }
  return data as T;
}

// ---------------------------------------------------------------------------
// Inventory service
// ---------------------------------------------------------------------------

export async function listEvents(signal?: AbortSignal): Promise<EventSummary[]> {
  const data = await request<{ events: EventSummary[] }>(INVENTORY_API_URL, '/events', { signal });
  return data.events;
}

export function getEvent(id: string, signal?: AbortSignal): Promise<EventSummary> {
  return request<EventSummary>(INVENTORY_API_URL, `/events/${encodeURIComponent(id)}`, { signal });
}

export function createEvent(input: CreateEventInput, signal?: AbortSignal): Promise<EventSummary> {
  return request<EventSummary>(INVENTORY_API_URL, '/events', { method: 'POST', body: input, signal });
}

export function getInventoryAdminOverview(signal?: AbortSignal): Promise<InventoryAdminOverview> {
  return request<InventoryAdminOverview>(INVENTORY_API_URL, '/admin/overview', { signal });
}

export function getOrderAdminOverview(signal?: AbortSignal): Promise<OrderAdminOverview> {
  return request<OrderAdminOverview>(ORDER_API_URL, '/admin/overview', { signal });
}

export function getNotificationAdminOverview(signal?: AbortSignal): Promise<NotificationAdminOverview> {
  return request<NotificationAdminOverview>(NOTIFICATION_API_URL, '/admin/overview', { signal });
}

export function createReservation(
  input: { eventId: string; quantity: number },
  idempotencyKey: string,
): Promise<Reservation> {
  return request<Reservation>(INVENTORY_API_URL, '/reservations', {
    method: 'POST',
    idempotencyKey,
    body: { eventId: input.eventId, userId: getUserId(), quantity: input.quantity },
  });
}

export function joinWaitlist(
  input: { eventId: string; quantity: number },
  idempotencyKey: string,
): Promise<WaitlistEntry> {
  return request<WaitlistEntry>(INVENTORY_API_URL, '/waitlist', {
    method: 'POST',
    idempotencyKey,
    body: { eventId: input.eventId, userId: getUserId(), quantity: input.quantity },
  });
}

// ---------------------------------------------------------------------------
// Order service
// ---------------------------------------------------------------------------

export function getOrder(id: string, signal?: AbortSignal): Promise<Order> {
  return request<Order>(ORDER_API_URL, `/orders/${encodeURIComponent(id)}`, { signal });
}

export function payOrder(orderId: string, idempotencyKey: string): Promise<PaymentInitiation> {
  return request<PaymentInitiation>(ORDER_API_URL, `/orders/${encodeURIComponent(orderId)}/pay`, {
    method: 'POST',
    idempotencyKey,
    body: {},
  });
}

async function listMyOrders(signal?: AbortSignal): Promise<Order[]> {
  const params = new URLSearchParams({ userId: getUserId(), limit: '20' });
  const data = await request<{ items: Order[] }>(ORDER_API_URL, `/orders?${params}`, { signal });
  return data.items;
}

/**
 * A reservation creates a hold in inventory-service; the matching order is created
 * asynchronously by order-service (via Kafka). Poll the user's orders until it appears.
 */
export async function waitForOrderByHold(
  holdId: string,
  opts: { timeoutMs?: number; intervalMs?: number; signal?: AbortSignal } = {},
): Promise<Order> {
  const { timeoutMs = 20_000, intervalMs = 700, signal } = opts;
  const deadline = Date.now() + timeoutMs;

  while (Date.now() < deadline) {
    if (signal?.aborted) throw new DOMException('Aborted', 'AbortError');
    const orders = await listMyOrders(signal);
    const match = orders.find((o) => o.holdId === holdId);
    if (match) return match;
    await new Promise((r) => setTimeout(r, intervalMs));
  }
  throw new ApiError(
    504,
    'ORDER_TIMEOUT',
    'Your seats are held, but the order is taking longer than usual to appear. Please try again in a moment.',
  );
}
