import { useCallback, useEffect, useState } from 'react';
import { Link } from 'react-router-dom';
import { errorMessage, formatPrice, listEvents, type EventSummary } from '../api/client';
import { LockMark } from '../components/Layout';
import Spinner from '../components/Spinner';
import StatusBadge, { availabilityStatus } from '../components/StatusBadge';

export function formatEventDate(iso: string): { month: string; day: string; full: string } {
  const d = new Date(iso);
  return {
    month: d.toLocaleDateString(undefined, { month: 'short' }).toUpperCase(),
    day: d.toLocaleDateString(undefined, { day: '2-digit' }),
    full: d.toLocaleString(undefined, {
      weekday: 'short',
      month: 'short',
      day: 'numeric',
      hour: 'numeric',
      minute: '2-digit',
    }),
  };
}

type LoadState =
  | { kind: 'loading' }
  | { kind: 'error'; message: string }
  | { kind: 'ready'; events: EventSummary[] };

export default function EventListPage() {
  const [state, setState] = useState<LoadState>({ kind: 'loading' });
  const [attempt, setAttempt] = useState(0);

  const retry = useCallback(() => {
    setState({ kind: 'loading' });
    setAttempt((n) => n + 1);
  }, []);

  useEffect(() => {
    const controller = new AbortController();
    listEvents(controller.signal)
      .then((events) => setState({ kind: 'ready', events }))
      .catch((err: unknown) => {
        if (controller.signal.aborted) return;
        setState({ kind: 'error', message: errorMessage(err) });
      });
    return () => controller.abort();
  }, [attempt]);

  return (
    <>
      <section className="hero">
        <div className="hero-inner">
          <p className="eyebrow">
            <LockMark size={18} /> Flash-sale ticketing
          </p>
          <h1 className="hero-title">SeatLock</h1>
          <p className="hero-sub">
            When the tickets drop, your seats are locked in the instant you click. No double-booking, no
            lost carts, just a few minutes to pay.
          </p>
        </div>
      </section>

      <section className="page" aria-labelledby="events-heading">
        <div className="section-head">
          <h2 id="events-heading" className="section-title">
            Upcoming events
          </h2>
          {state.kind === 'ready' && <span className="muted">{state.events.length} on sale</span>}
        </div>

        {state.kind === 'loading' && <Spinner label="Loading events..." />}

        {state.kind === 'error' && (
          <div className="notice notice-error" role="alert">
            <div>
              <strong>Couldn&apos;t load events.</strong>
              <p>{state.message}</p>
            </div>
            <button type="button" className="btn btn-secondary" onClick={retry}>
              Try again
            </button>
          </div>
        )}

        {state.kind === 'ready' && state.events.length === 0 && (
          <p className="empty">No events are on sale right now. Check back soon.</p>
        )}

        {state.kind === 'ready' && state.events.length > 0 && (
          <ul className="event-list">
            {state.events.map((event) => {
              const date = formatEventDate(event.startsAt);
              const status = availabilityStatus(event.available, event.totalTickets);
              return (
                <li key={event.id}>
                  <Link to={`/events/${event.id}`} className="event-row">
                    <div className="event-date" aria-hidden="true">
                      <span>{date.month}</span>
                      <strong>{date.day}</strong>
                    </div>
                    <div className="event-main">
                      <h3>{event.name}</h3>
                      <p className="muted">
                        {date.full}
                        {event.venue ? ` \u00b7 ${event.venue}` : ''}
                      </p>
                    </div>
                    <div className="event-meta">
                      <StatusBadge status={status} />
                      <span className="muted small">
                        {event.available > 0 ? `${event.available} left` : 'Join the waitlist'}
                      </span>
                    </div>
                    <div className="event-price">
                      <strong>{formatPrice(event.unitPriceCents)}</strong>
                      <span className="muted small">per ticket</span>
                    </div>
                  </Link>
                </li>
              );
            })}
          </ul>
        )}
      </section>
    </>
  );
}
