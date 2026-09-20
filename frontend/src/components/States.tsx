/**
 * The non-success states.
 *
 * Five of them, kept visually distinct because conflating them is how a UI ends
 * up lying: "no data" is a fact about the session, "stale" is a fact about the
 * stream, and "error" is a fact about us. A user acts differently on each.
 */

import clsx from 'clsx';

interface StateProps {
  title: string;
  detail?: string | null;
  className?: string;
  action?: React.ReactNode;
}

function StateFrame({
  title,
  detail,
  className,
  action,
  tone,
  glyph,
}: StateProps & { tone: string; glyph: React.ReactNode }) {
  return (
    <div
      className={clsx(
        'flex flex-col items-center justify-center gap-2 px-6 py-10 text-center',
        className,
      )}
      role="status"
    >
      <div className={clsx('mb-1', tone)}>{glyph}</div>
      <p className="text-sm font-medium text-content-secondary">{title}</p>
      {detail && <p className="max-w-sm text-xs text-content-tertiary">{detail}</p>}
      {action && <div className="mt-2">{action}</div>}
    </div>
  );
}

const glyph = (d: string) => (
  <svg
    viewBox="0 0 24 24"
    fill="none"
    stroke="currentColor"
    strokeWidth="1.25"
    strokeLinecap="round"
    strokeLinejoin="round"
    className="h-6 w-6"
    aria-hidden="true"
  >
    <path d={d} />
  </svg>
);

/**
 * The session genuinely does not carry this channel.
 *
 * This is the state that keeps the project honest: when upstream has no
 * telemetry for a session, we say so rather than drawing something plausible.
 */
export function EmptyState({ title, detail, className, action }: StateProps) {
  return (
    <StateFrame
      title={title}
      detail={detail}
      className={className}
      action={action}
      tone="text-content-disabled"
      glyph={glyph('M3 12h4l2-6 3 12 2.5-7 1.5 3h5')}
    />
  );
}

/** We failed. Shows what failed, and offers a retry when one is possible. */
export function ErrorState({ title, detail, className, action }: StateProps) {
  return (
    <StateFrame
      title={title}
      detail={detail}
      className={className}
      action={action}
      tone="text-status-error"
      glyph={glyph('M12 8v5M12 16.5v.01M10.3 3.9 2.6 17.2A1.9 1.9 0 0 0 4.3 20h15.4a1.9 1.9 0 0 0 1.7-2.8L13.7 3.9a1.9 1.9 0 0 0-3.4 0Z')}
    />
  );
}

/** The socket is open but nothing recent has arrived. */
export function StaleState({ title, detail, className, action }: StateProps) {
  return (
    <StateFrame
      title={title}
      detail={detail}
      className={className}
      action={action}
      tone="text-status-warning"
      glyph={glyph('M12 7v5l3 2M12 21a9 9 0 1 1 0-18 9 9 0 0 1 0 18Z')}
    />
  );
}

/** No stream at all. */
export function DisconnectedState({ title, detail, className, action }: StateProps) {
  return (
    <StateFrame
      title={title}
      detail={detail}
      className={className}
      action={action}
      tone="text-content-disabled"
      glyph={glyph('M3 3l18 18M8.5 16.4a5 5 0 0 1 7 0M5 13a10 10 0 0 1 3.5-2.3M19 13a10 10 0 0 0-7.5-2.9M2 9a15 15 0 0 1 5-3.2M22 9a15 15 0 0 0-9-3.8M12 20h.01')}
    />
  );
}

/** A small, quiet retry affordance for use in the `action` slot. */
export function RetryButton({ onClick, label = 'RETRY' }: { onClick: () => void; label?: string }) {
  return (
    <button
      type="button"
      onClick={onClick}
      className="rounded border border-line bg-surface-inset px-3 py-1.5 text-2xs font-medium uppercase tracking-label text-content-secondary transition-colors duration-fast hover:border-line-strong hover:bg-surface-hover hover:text-content-primary"
    >
      {label}
    </button>
  );
}
