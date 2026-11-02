import { useEffect, useRef, useState } from 'react';
import { Link, useLocation, useNavigate, useParams } from 'react-router-dom';
import {
  ApiError,
  errorMessage,
  formatPrice,
  getEvent,
  getOrder,
  newUuid,
  payOrder,
  type EventSummary,
  type Order,
} from '../api/client';
import Spinner from '../components/Spinner';
import StatusBadge from '../components/StatusBadge';

type LoadState =
  | { kind: 'loading' }
  | { kind: 'error'; message: string; notFound: boolean }
  | { kind: 'ready'; order: Order };

function useCountdown(expiresAt: string | undefined): number | null {
  const [remaining, setRemaining] = useState<number | null>(null);

  useEffect(() => {
    if (!expiresAt) return;
    const target = new Date(expiresAt).getTime();
    const tick = () => setRemaining(Math.max(0, Math.floor((target - Date.now()) / 1000)));
    tick();
    const timer = setInterval(tick, 1000);
    return () => clearInterval(timer);
  }, [expiresAt]);

  return remaining;
}

function formatClock(totalSeconds: number): string {
  const m = Math.floor(totalSeconds / 60);
  const s = totalSeconds % 60;
  return `${m}:${s.toString().padStart(2, '0')}`;
}

export default function PaymentPage() {
  const { orderId = '' } = useParams();
  const navigate = useNavigate();
  const location = useLocation();
  const expiresAt = (location.state as { expiresAt?: string } | null)?.expiresAt;

  const [load, setLoad] = useState<LoadState>({ kind: 'loading' });
  const [event, setEvent] = useState<EventSummary | null>(null);
  const [paying, setPaying] = useState(false);
  const [payError, setPayError] = useState<string | null>(null);
  const payKey = useRef<string | null>(null);

  const remaining = useCountdown(expiresAt);
  const holdExpired = remaining === 0;

  useEffect(() => {
    const controller = new AbortController();
    getOrder(orderId, controller.signal)
      .then((order) => {
        setLoad({ kind: 'ready', order });
        // Event name is a nicety; the page works without it.
        getEvent(order.eventId, controller.signal)
          .then(setEvent)
          .catch(() => undefined);
      })
      .catch((err: unknown) => {
        if (controller.signal.aborted) return;
        setLoad({
          kind: 'error',
          message: errorMessage(err),
          notFound: err instanceof ApiError && err.status === 404,
        });
      });
    return () => controller.abort();
  }, [orderId]);

  const handlePay = async () => {
    setPayError(null);
    setPaying(true);
    try {
      // Same key on retry => the server replays the original result instead of charging twice.
      payKey.current ??= newUuid();
      await payOrder(orderId, payKey.current);
      navigate(`/orders/${orderId}`);
    } catch (err) {
      if (err instanceof ApiError && err.status >= 400 && err.status < 500) payKey.current = null;
      setPayError(errorMessage(err));
      setPaying(false);
    }
  };

  if (load.kind === 'loading') {
    return (
      <div className="page narrow">
        <Spinner label="Loading your order..." />
      </div>
    );
  }

  if (load.kind === 'error') {
    return (
      <div className="page narrow">
        <div className="notice notice-error" role="alert">
          <div>
            <strong>{load.notFound ? 'Order not found.' : "Couldn't load your order."}</strong>
            <p>{load.message}</p>
          </div>
        </div>
        <Link className="text-link" to="/">
          &larr; Back to events
        </Link>
      </div>
    );
  }

  const { order } = load;
  const payable = order.status === 'PENDING_PAYMENT';

  return (
    <div className="page narrow">
      <p className="eyebrow-dark">Step 2 of 3 &middot; Payment</p>
      <h1 className="page-title">Complete your purchase</h1>

      <section className="card pay-card" aria-label="Payment">
        <div className="row-between">
          <h2 className="card-title">{event?.name ?? 'Your tickets'}</h2>
          <StatusBadge status={order.status} />
        </div>

        <dl className="summary">
          <div>
            <dt>Tickets</dt>
            <dd>{order.quantity}</dd>
          </div>
          <div>
            <dt>Order</dt>
            <dd className="mono">{order.id.slice(0, 8)}</dd>
          </div>
          <div className="summary-total">
            <dt>Total due</dt>
            <dd>{formatPrice(order.amountCents)}</dd>
          </div>
        </dl>

        {payable && remaining !== null && (
          <p className={`hold-timer ${remaining <= 60 ? 'hold-timer-low' : ''}`} role="timer">
            {holdExpired
              ? 'Your hold has expired. The seats may have been released.'
              : `Seats held for ${formatClock(remaining)}`}
          </p>
        )}

        <div className="mock-card" aria-hidden="true">
          <span>Mock payment method</span>
          <span className="mono">&bull;&bull;&bull;&bull; &bull;&bull;&bull;&bull; &bull;&bull;&bull;&bull; 4242</span>
        </div>

        {payError && (
          <div className="notice notice-error" role="alert">
            <p>{payError}</p>
          </div>
        )}

        {payable ? (
          <button
            type="button"
            className="btn btn-primary btn-block"
            onClick={handlePay}
            disabled={paying || holdExpired}
          >
            {paying ? 'Processing...' : `Pay ${formatPrice(order.amountCents)} now`}
          </button>
        ) : (
          <div className="notice">
            <p>This order is no longer awaiting payment.</p>
            <Link className="text-link" to={`/orders/${order.id}`}>
              View order status
            </Link>
          </div>
        )}
      </section>
    </div>
  );
}
