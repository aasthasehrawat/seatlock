import { useEffect, useState } from 'react';
import { Link, useParams } from 'react-router-dom';
import { ApiError, errorMessage, formatPrice, getOrder, isTerminal, type Order } from '../api/client';
import Spinner from '../components/Spinner';
import StatusBadge from '../components/StatusBadge';

const POLL_INTERVAL_MS = 1500;
/** Give up on transient errors after this many failures in a row. */
const MAX_CONSECUTIVE_ERRORS = 5;

const OUTCOMES = {
  PENDING_PAYMENT: {
    title: 'Processing your payment',
    text: 'Hang tight. We are confirming your payment with the bank.',
  },
  CONFIRMED: {
    title: "You're going!",
    text: 'Payment received and your seats are locked in. A confirmation is on its way.',
  },
  FAILED: {
    title: 'Payment failed',
    text: 'Your payment could not be completed and you have not been charged. The seats will return to sale.',
  },
  EXPIRED: {
    title: 'Hold expired',
    text: 'The payment window closed before payment was completed, so the seats were released.',
  },
} as const;

export default function OrderStatusPage() {
  const { orderId = '' } = useParams();
  const [order, setOrder] = useState<Order | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [fatal, setFatal] = useState(false);

  useEffect(() => {
    const controller = new AbortController();
    let timer: ReturnType<typeof setTimeout> | undefined;
    let failures = 0;

    const poll = async () => {
      try {
        const next = await getOrder(orderId, controller.signal);
        failures = 0;
        setError(null);
        setOrder(next);
        if (isTerminal(next.status)) return; // done: stop polling
      } catch (err) {
        if (controller.signal.aborted) return;
        failures += 1;
        setError(errorMessage(err));
        const permanent = err instanceof ApiError && err.status >= 400 && err.status < 500;
        if (permanent || failures >= MAX_CONSECUTIVE_ERRORS) {
          setFatal(true);
          return;
        }
      }
      timer = setTimeout(poll, POLL_INTERVAL_MS);
    };

    void poll();
    return () => {
      controller.abort();
      clearTimeout(timer);
    };
  }, [orderId]);

  if (!order) {
    if (fatal) {
      return (
        <div className="page narrow">
          <div className="notice notice-error" role="alert">
            <div>
              <strong>Couldn&apos;t load this order.</strong>
              <p>{error}</p>
            </div>
          </div>
          <Link className="text-link" to="/">
            &larr; Back to events
          </Link>
        </div>
      );
    }
    return (
      <div className="page narrow">
        <Spinner label="Checking your order..." />
      </div>
    );
  }

  const outcome = OUTCOMES[order.status];
  const pending = order.status === 'PENDING_PAYMENT';

  return (
    <div className="page narrow">
      <p className="eyebrow-dark">Step 3 of 3 &middot; Confirmation</p>

      <div className={`outcome outcome-${order.status.toLowerCase()}`} role="status" aria-live="polite">
        {pending ? <Spinner size="sm" /> : <span className="outcome-icon" aria-hidden="true">{order.status === 'CONFIRMED' ? '\u2713' : '!'}</span>}
        <div>
          <h1 className="page-title">{outcome.title}</h1>
          <p className="muted">{outcome.text}</p>
        </div>
      </div>

      {error && !fatal && (
        <p className="muted small" role="alert">
          Having trouble reaching the server. Retrying...
        </p>
      )}

      <dl className="summary">
        <div>
          <dt>Status</dt>
          <dd>
            <StatusBadge status={order.status} />
          </dd>
        </div>
        <div>
          <dt>Tickets</dt>
          <dd>{order.quantity}</dd>
        </div>
        <div>
          <dt>Order</dt>
          <dd className="mono">{order.id}</dd>
        </div>
        <div className="summary-total">
          <dt>Total</dt>
          <dd>{formatPrice(order.amountCents)}</dd>
        </div>
      </dl>

      {!pending && (
        <div className="actions">
          <Link className="btn btn-primary" to="/">
            {order.status === 'CONFIRMED' ? 'Browse more events' : 'Back to events'}
          </Link>
          {order.status !== 'CONFIRMED' && (
            <Link className="btn btn-secondary" to={`/events/${order.eventId}`}>
              Try this event again
            </Link>
          )}
        </div>
      )}
    </div>
  );
}
