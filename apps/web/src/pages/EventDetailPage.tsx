import { useCallback, useEffect, useRef, useState } from 'react';
import { Link, useNavigate, useParams } from 'react-router-dom';
import {
  ApiError,
  createReservation,
  errorMessage,
  formatPrice,
  getEvent,
  joinWaitlist,
  newUuid,
  waitForOrderByHold,
  type EventSummary,
  type Reservation,
  type WaitlistEntry,
} from '../api/client';
import QuantitySelector from '../components/QuantitySelector';
import Spinner from '../components/Spinner';
import StatusBadge, { availabilityStatus } from '../components/StatusBadge';
import { formatEventDate } from './EventListPage';

/** Upper bound shown in the UI. The server enforces its own per-reservation limit. */
const MAX_PER_ORDER = 6;

type LoadState =
  | { kind: 'loading' }
  | { kind: 'error'; message: string; notFound: boolean }
  | { kind: 'ready'; event: EventSummary };

type ReservePhase = 'idle' | 'reserving' | 'preparing';

export default function EventDetailPage() {
  const { id = '' } = useParams();
  const navigate = useNavigate();

  const [load, setLoad] = useState<LoadState>({ kind: 'loading' });
  const [attempt, setAttempt] = useState(0);
  const [quantity, setQuantity] = useState(1);

  // Reservation flow
  const [phase, setPhase] = useState<ReservePhase>('idle');
  const [reserveError, setReserveError] = useState<string | null>(null);

  // Waitlist flow
  const [joining, setJoining] = useState(false);
  const [waitlistError, setWaitlistError] = useState<string | null>(null);
  const [waitlisted, setWaitlisted] = useState<WaitlistEntry | null>(null);

  // One idempotency key per user intent. It is reused when retrying after a network failure so the
  // server can de-duplicate, and discarded when the intent changes (new quantity) or definitively fails.
  const reserveKey = useRef<string | null>(null);
  const waitlistKey = useRef<string | null>(null);
  const reservation = useRef<Reservation | null>(null);
  const abortRef = useRef<AbortController | null>(null);

  const resetIntent = () => {
    reserveKey.current = null;
    waitlistKey.current = null;
    reservation.current = null;
  };

  useEffect(() => () => abortRef.current?.abort(), []);

  useEffect(() => {
    const controller = new AbortController();
    getEvent(id, controller.signal)
      .then((event) => setLoad({ kind: 'ready', event }))
      .catch((err: unknown) => {
        if (controller.signal.aborted) return;
        setLoad({
          kind: 'error',
          message: errorMessage(err),
          notFound: err instanceof ApiError && err.status === 404,
        });
      });
    return () => controller.abort();
  }, [id, attempt]);

  const refreshEvent = useCallback(() => {
    getEvent(id)
      .then((event) => setLoad({ kind: 'ready', event }))
      .catch(() => {
        /* keep showing the last known event */
      });
  }, [id]);

  const handleQuantity = (next: number) => {
    setQuantity(next);
    resetIntent();
    setReserveError(null);
    setWaitlistError(null);
  };

  const handleReserve = async () => {
    if (load.kind !== 'ready') return;
    setReserveError(null);
    abortRef.current?.abort();
    const controller = new AbortController();
    abortRef.current = controller;

    try {
      // Step 1: lock the seats (skipped when retrying only the order lookup).
      let held = reservation.current;
      if (!held) {
        setPhase('reserving');
        reserveKey.current ??= newUuid();
        held = await createReservation({ eventId: load.event.id, quantity }, reserveKey.current);
        reservation.current = held;
      }

      // Step 2: the order is created asynchronously from the hold; wait for it, then pay.
      setPhase('preparing');
      const order = await waitForOrderByHold(held.holdId, { signal: controller.signal });
      navigate(`/pay/${order.id}`, { state: { expiresAt: held.expiresAt } });
    } catch (err) {
      if (controller.signal.aborted) return;
      setPhase('idle');
      setReserveError(errorMessage(err));
      if (err instanceof ApiError && err.status >= 400 && err.status < 500 && !reservation.current) {
        // Definitive rejection: the next click is a fresh attempt with a new key.
        reserveKey.current = null;
      }
      if (err instanceof ApiError && err.isSoldOut) refreshEvent();
    }
  };

  const handleWaitlist = async () => {
    if (load.kind !== 'ready') return;
    setWaitlistError(null);
    setJoining(true);
    try {
      waitlistKey.current ??= newUuid();
      const entry = await joinWaitlist({ eventId: load.event.id, quantity }, waitlistKey.current);
      setWaitlisted(entry);
    } catch (err) {
      if (err instanceof ApiError && err.status >= 400 && err.status < 500) waitlistKey.current = null;
      if (err instanceof ApiError && err.code === 'TICKETS_AVAILABLE') refreshEvent();
      setWaitlistError(errorMessage(err));
    } finally {
      setJoining(false);
    }
  };

  // ------------------------------------------------------------------ render

  if (load.kind === 'loading') {
    return (
      <div className="page">
        <Spinner label="Loading event..." />
      </div>
    );
  }

  if (load.kind === 'error') {
    return (
      <div className="page narrow">
        <Link className="text-link" to="/">
          &larr; All events
        </Link>
        <div className="notice notice-error" role="alert">
          <div>
            <strong>{load.notFound ? 'Event not found.' : "Couldn't load this event."}</strong>
            <p>{load.message}</p>
          </div>
          {!load.notFound && (
            <button
              type="button"
              className="btn btn-secondary"
              onClick={() => {
                setLoad({ kind: 'loading' });
                setAttempt((n) => n + 1);
              }}
            >
              Try again
            </button>
          )}
        </div>
      </div>
    );
  }

  const { event } = load;
  const soldOut = event.available <= 0;
  const date = formatEventDate(event.startsAt);
  const maxQty = Math.max(1, Math.min(MAX_PER_ORDER, soldOut ? MAX_PER_ORDER : event.available));
  const safeQty = Math.min(quantity, maxQty);
  const total = event.unitPriceCents * safeQty;
  const busy = phase !== 'idle';

  return (
    <div className="page detail">
      <div className="detail-info">
        <Link className="text-link" to="/">
          &larr; All events
        </Link>
        <div className="detail-badge">
          <StatusBadge status={availabilityStatus(event.available, event.totalTickets)} />
        </div>
        <h1 className="page-title">{event.name}</h1>
        <p className="detail-when">{date.full}</p>
        {event.venue && <p className="muted">{event.venue}</p>}
        {event.description && <p className="detail-desc">{event.description}</p>}

        <dl className="facts">
          <div>
            <dt>Price</dt>
            <dd>{formatPrice(event.unitPriceCents)}</dd>
          </div>
          <div>
            <dt>Available</dt>
            <dd>
              {event.available} <span className="muted">/ {event.totalTickets}</span>
            </dd>
          </div>
        </dl>
      </div>

      <aside className="card ticket-card" aria-label="Ticket selector">
        {!soldOut && (
          <>
            <h2 className="card-title">Choose tickets</h2>
            <div className="row-between">
              <span>Quantity</span>
              <QuantitySelector value={safeQty} max={maxQty} disabled={busy} onChange={handleQuantity} />
            </div>
            <div className="row-between total">
              <span>Total</span>
              <strong>{formatPrice(total)}</strong>
            </div>

            {reserveError && (
              <div className="notice notice-error" role="alert">
                <p>{reserveError}</p>
              </div>
            )}

            <button type="button" className="btn btn-primary btn-block" onClick={handleReserve} disabled={busy}>
              {phase === 'reserving' && 'Locking your seats...'}
              {phase === 'preparing' && 'Preparing checkout...'}
              {phase === 'idle' && (reservation.current ? 'Continue to payment' : 'Reserve tickets')}
            </button>
            <p className="muted small center">
              Seats are held for a few minutes while you pay. You won&apos;t be charged until you confirm.
            </p>
          </>
        )}

        {soldOut && !waitlisted && (
          <>
            <div className="soldout-head">
              <StatusBadge status="SOLD_OUT" />
              <h2 className="card-title">This event is sold out</h2>
            </div>
            <p className="muted">
              Held seats are released when payments time out. Join the waitlist and we&apos;ll offer you
              tickets first-come, first-served.
            </p>
            <div className="row-between">
              <span>Tickets wanted</span>
              <QuantitySelector value={safeQty} max={maxQty} disabled={joining} onChange={handleQuantity} />
            </div>

            {waitlistError && (
              <div className="notice notice-error" role="alert">
                <p>{waitlistError}</p>
              </div>
            )}

            <button
              type="button"
              className="btn btn-primary btn-block"
              onClick={handleWaitlist}
              disabled={joining}
            >
              {joining ? 'Joining...' : 'Join the waitlist'}
            </button>
          </>
        )}

        {soldOut && waitlisted && (
          <div className="success-block" role="status">
            <h2 className="card-title">You&apos;re on the waitlist</h2>
            <p>
              Position <strong>#{waitlisted.position}</strong> for {waitlisted.quantity}{' '}
              {waitlisted.quantity === 1 ? 'ticket' : 'tickets'}.
            </p>
            <p className="muted small">If seats free up, you&apos;ll get a time-limited hold to complete payment.</p>
          </div>
        )}
      </aside>
    </div>
  );
}
