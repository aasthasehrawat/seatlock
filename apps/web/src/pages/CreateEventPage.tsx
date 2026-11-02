import { useState, type FormEvent } from 'react';
import { Link, useNavigate } from 'react-router-dom';
import { createEvent, errorMessage, formatPrice, type CreateEventInput } from '../api/client';

interface FormValues {
  name: string;
  venue: string;
  description: string;
  startsAt: string;
  totalTickets: string;
  unitPrice: string;
}

type FieldErrors = Partial<Record<keyof FormValues, string>>;

const MAX_TICKETS = 100_000;
const MAX_INT4 = 2_147_483_647;

/** Format a Date as the `YYYY-MM-DDTHH:mm` string a datetime-local input expects (local time). */
function toLocalInputValue(d: Date): string {
  const pad = (n: number) => String(n).padStart(2, '0');
  return `${d.getFullYear()}-${pad(d.getMonth() + 1)}-${pad(d.getDate())}T${pad(d.getHours())}:${pad(d.getMinutes())}`;
}

function defaultStart(): string {
  const d = new Date();
  d.setDate(d.getDate() + 30);
  d.setHours(19, 0, 0, 0);
  return toLocalInputValue(d);
}

/**
 * datetime-local has no timezone, so interpret it in the browser's zone and emit an ISO string
 * with an explicit offset (UTC "Z"), which is what the API requires.
 */
function toIsoWithOffset(local: string): string | null {
  const d = new Date(local);
  return Number.isNaN(d.getTime()) ? null : d.toISOString();
}

function validate(v: FormValues): { errors: FieldErrors; input?: CreateEventInput } {
  const errors: FieldErrors = {};

  const name = v.name.trim();
  if (!name) errors.name = 'Name is required.';
  else if (name.length > 200) errors.name = 'Name must be 200 characters or fewer.';

  if (v.venue.trim().length > 500) errors.venue = 'Venue must be 500 characters or fewer.';
  if (v.description.trim().length > 5000) errors.description = 'Description must be 5,000 characters or fewer.';

  let startsAt: string | null = null;
  if (!v.startsAt) errors.startsAt = 'Start date and time is required.';
  else {
    startsAt = toIsoWithOffset(v.startsAt);
    if (!startsAt) errors.startsAt = 'Enter a valid date and time.';
  }

  const ticketsRaw = v.totalTickets.trim();
  const tickets = Number(ticketsRaw);
  if (!ticketsRaw) errors.totalTickets = 'Total tickets is required.';
  else if (!Number.isInteger(tickets) || tickets < 1) errors.totalTickets = 'Enter a whole number of at least 1.';
  else if (tickets > MAX_TICKETS) errors.totalTickets = `Maximum is ${MAX_TICKETS.toLocaleString()} tickets.`;

  let cents = 0;
  const priceRaw = v.unitPrice.trim();
  if (priceRaw) {
    if (!/^\d+(\.\d{1,2})?$/.test(priceRaw)) {
      errors.unitPrice = 'Enter a price in dollars, like 49 or 49.99.';
    } else {
      cents = Math.round(Number(priceRaw) * 100);
      if (cents > MAX_INT4) errors.unitPrice = 'Price is too large.';
    }
  }

  if (Object.keys(errors).length > 0 || !startsAt) return { errors };

  const input: CreateEventInput = {
    name,
    startsAt,
    totalTickets: tickets,
    unitPriceCents: cents,
  };
  const venue = v.venue.trim();
  const description = v.description.trim();
  if (venue) input.venue = venue;
  if (description) input.description = description;
  return { errors, input };
}

export default function CreateEventPage() {
  const navigate = useNavigate();
  const [values, setValues] = useState<FormValues>({
    name: '',
    venue: '',
    description: '',
    startsAt: defaultStart(),
    totalTickets: '100',
    unitPrice: '25',
  });
  const [errors, setErrors] = useState<FieldErrors>({});
  const [submitting, setSubmitting] = useState(false);
  const [submitError, setSubmitError] = useState<string | null>(null);

  const set = (key: keyof FormValues) => (e: { target: { value: string } }) => {
    setValues((prev) => ({ ...prev, [key]: e.target.value }));
    if (errors[key]) setErrors((prev) => ({ ...prev, [key]: undefined }));
  };

  async function onSubmit(e: FormEvent) {
    e.preventDefault();
    if (submitting) return;
    setSubmitError(null);

    const result = validate(values);
    setErrors(result.errors);
    if (!result.input) {
      const first = Object.keys(result.errors)[0];
      if (first) document.getElementById(`field-${first}`)?.focus();
      return;
    }

    setSubmitting(true);
    try {
      const created = await createEvent(result.input);
      // Event is inserted into the inventory Postgres DB (events table) by inventory-service.
      navigate(`/admin?created=${encodeURIComponent(created.id)}&name=${encodeURIComponent(created.name)}`);
    } catch (err) {
      setSubmitError(
        `${errorMessage(err)} Make sure Docker is running (\`docker compose up -d\`) and the services are up (\`npm run dev\`).`,
      );
      setSubmitting(false);
    }
  }

  const priceCents = /^\d+(\.\d{1,2})?$/.test(values.unitPrice.trim())
    ? Math.round(Number(values.unitPrice.trim()) * 100)
    : null;

  return (
    <div className="page admin-page narrow-admin">
      <Link to="/admin" className="text-link">
        &larr; Back to dashboard
      </Link>
      <h1 className="page-title">New event</h1>
      <p className="muted create-lede">
        Publish an event and its ticket inventory. It is saved to the{' '}
        <strong>inventory Postgres database</strong> and goes on sale immediately.
      </p>

      <div className="notice demo-note">
        <p className="small">
          Admin create is unauthenticated in this demo. Rows live in the{' '}
          <code>events</code> table (container <code>seatlock-postgres-inventory</code>).
        </p>
      </div>

      <form className="form-grid" onSubmit={onSubmit} noValidate>
        <Field id="name" label="Event name" required error={errors.name} className="span-2">
          <input
            id="field-name"
            type="text"
            value={values.name}
            onChange={set('name')}
            maxLength={200}
            autoComplete="off"
            aria-invalid={!!errors.name}
            aria-describedby={errors.name ? 'err-name' : undefined}
            placeholder="Midnight Jazz at the Harbor"
          />
        </Field>

        <Field id="venue" label="Venue" error={errors.venue}>
          <input
            id="field-venue"
            type="text"
            value={values.venue}
            onChange={set('venue')}
            maxLength={500}
            autoComplete="off"
            aria-invalid={!!errors.venue}
            aria-describedby={errors.venue ? 'err-venue' : undefined}
            placeholder="Harbor Hall"
          />
        </Field>

        <Field id="startsAt" label="Starts at" required error={errors.startsAt} hint="Your local time zone.">
          <input
            id="field-startsAt"
            type="datetime-local"
            value={values.startsAt}
            onChange={set('startsAt')}
            aria-invalid={!!errors.startsAt}
            aria-describedby={errors.startsAt ? 'err-startsAt' : undefined}
          />
        </Field>

        <Field id="totalTickets" label="Total tickets" required error={errors.totalTickets}>
          <input
            id="field-totalTickets"
            type="number"
            inputMode="numeric"
            min={1}
            max={MAX_TICKETS}
            step={1}
            value={values.totalTickets}
            onChange={set('totalTickets')}
            aria-invalid={!!errors.totalTickets}
            aria-describedby={errors.totalTickets ? 'err-totalTickets' : undefined}
          />
        </Field>

        <Field
          id="unitPrice"
          label="Ticket price (USD)"
          error={errors.unitPrice}
          hint={priceCents !== null ? `Stored as ${formatPrice(priceCents)} per ticket. Leave blank for free.` : 'Leave blank for free.'}
        >
          <input
            id="field-unitPrice"
            type="text"
            inputMode="decimal"
            value={values.unitPrice}
            onChange={set('unitPrice')}
            aria-invalid={!!errors.unitPrice}
            aria-describedby={errors.unitPrice ? 'err-unitPrice' : undefined}
            placeholder="25.00"
          />
        </Field>

        <Field id="description" label="Description" error={errors.description} className="span-2">
          <textarea
            id="field-description"
            rows={4}
            value={values.description}
            onChange={set('description')}
            maxLength={5000}
            aria-invalid={!!errors.description}
            aria-describedby={errors.description ? 'err-description' : undefined}
            placeholder="What should attendees know?"
          />
        </Field>

        {submitError && (
          <div className="notice notice-error span-2" role="alert">
            <div>
              <strong>Couldn&apos;t create the event.</strong>
              <p>{submitError}</p>
            </div>
          </div>
        )}

        <div className="form-actions span-2">
          <button type="submit" className="btn btn-primary" disabled={submitting}>
            {submitting ? 'Creating...' : 'Create event'}
          </button>
          <Link to="/admin" className="btn btn-secondary">
            Cancel
          </Link>
        </div>
      </form>
    </div>
  );
}

function Field({
  id,
  label,
  required,
  error,
  hint,
  className,
  children,
}: {
  id: string;
  label: string;
  required?: boolean;
  error?: string;
  hint?: string;
  className?: string;
  children: React.ReactNode;
}) {
  return (
    <div className={`field${className ? ` ${className}` : ''}`}>
      <label htmlFor={`field-${id}`}>
        {label}
        {required && <span className="req" aria-hidden="true"> *</span>}
      </label>
      {children}
      {error ? (
        <p className="field-error" id={`err-${id}`} role="alert">
          {error}
        </p>
      ) : (
        hint && <p className="field-hint">{hint}</p>
      )}
    </div>
  );
}
