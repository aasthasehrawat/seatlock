interface SpinnerProps {
  label?: string;
  size?: 'sm' | 'md';
}

export default function Spinner({ label, size = 'md' }: SpinnerProps) {
  return (
    <div className="spinner-wrap" role="status" aria-live="polite">
      <span className={`spinner spinner-${size}`} aria-hidden="true" />
      {label ? <span className="spinner-label">{label}</span> : <span className="sr-only">Loading</span>}
    </div>
  );
}
