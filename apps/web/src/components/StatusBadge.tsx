export type BadgeStatus =
  | 'PENDING_PAYMENT'
  | 'CONFIRMED'
  | 'FAILED'
  | 'EXPIRED'
  | 'ON_SALE'
  | 'LOW_STOCK'
  | 'SOLD_OUT';

const BADGES: Record<BadgeStatus, { label: string; tone: 'ok' | 'warn' | 'bad' | 'neutral' }> = {
  PENDING_PAYMENT: { label: 'Awaiting payment', tone: 'warn' },
  CONFIRMED: { label: 'Confirmed', tone: 'ok' },
  FAILED: { label: 'Payment failed', tone: 'bad' },
  EXPIRED: { label: 'Hold expired', tone: 'neutral' },
  ON_SALE: { label: 'On sale', tone: 'ok' },
  LOW_STOCK: { label: 'Selling fast', tone: 'warn' },
  SOLD_OUT: { label: 'Sold out', tone: 'bad' },
};

/** Availability helper shared by the list and detail pages. */
export function availabilityStatus(available: number, total: number): BadgeStatus {
  if (available <= 0) return 'SOLD_OUT';
  if (available <= Math.max(5, Math.ceil(total * 0.1))) return 'LOW_STOCK';
  return 'ON_SALE';
}

export default function StatusBadge({ status }: { status: BadgeStatus }) {
  const { label, tone } = BADGES[status];
  return <span className={`badge badge-${tone}`}>{label}</span>;
}
