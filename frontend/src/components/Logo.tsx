/**
 * RaceStream wordmark.
 *
 * The motif is a telemetry trace, not a chequered flag: this is engineering
 * software, and the mark should say so. The trace doubles as the "signal"
 * reference in the name.
 */

interface LogoProps {
  /** Collapsed rail shows the mark alone. */
  variant?: 'full' | 'mark';
  className?: string;
}

export function Logo({ variant = 'full', className }: LogoProps) {
  return (
    <div className={`flex items-center gap-2.5 ${className ?? ''}`}>
      <svg
        viewBox="0 0 32 32"
        className="h-7 w-7 shrink-0"
        role="img"
        aria-label="RaceStream"
      >
        <rect width="32" height="32" rx="7" className="fill-surface-card" />
        <rect
          width="31"
          height="31"
          x="0.5"
          y="0.5"
          rx="6.5"
          fill="none"
          className="stroke-line"
        />
        <path
          d="M4 20 L9 20 L12 11 L16 25 L20 8 L23 20 L28 20"
          fill="none"
          stroke="#E10600"
          strokeWidth="2.4"
          strokeLinecap="round"
          strokeLinejoin="round"
        />
      </svg>

      {variant === 'full' && (
        // Two weights in one word: "RACE" carries the emphasis, "STREAM" the
        // system. Tight tracking keeps it reading as a single mark.
        <span className="select-none text-[0.9375rem] font-semibold leading-none tracking-tight">
          <span className="text-content-primary">RACE</span>
          <span className="text-content-tertiary">STREAM</span>
        </span>
      )}
    </div>
  );
}
