interface QuantitySelectorProps {
  value: number;
  min?: number;
  max: number;
  disabled?: boolean;
  onChange: (next: number) => void;
}

export default function QuantitySelector({
  value,
  min = 1,
  max,
  disabled = false,
  onChange,
}: QuantitySelectorProps) {
  const dec = () => onChange(Math.max(min, value - 1));
  const inc = () => onChange(Math.min(max, value + 1));

  return (
    <div className="qty" role="group" aria-label="Ticket quantity">
      <button
        type="button"
        className="qty-btn"
        onClick={dec}
        disabled={disabled || value <= min}
        aria-label="Decrease quantity"
      >
        &minus;
      </button>
      <output className="qty-value" aria-live="polite">
        {value}
      </output>
      <button
        type="button"
        className="qty-btn"
        onClick={inc}
        disabled={disabled || value >= max}
        aria-label="Increase quantity"
      >
        +
      </button>
    </div>
  );
}
