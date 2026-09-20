/**
 * Status primitives.
 *
 * Accessibility rule enforced here rather than left to each caller: a status is
 * never communicated by colour alone. Every indicator renders a text label, and
 * the dot is decorative (`aria-hidden`) with the meaning carried by the text.
 */

import clsx from 'clsx';
import type { ConnectionState, HealthState, StreamMode } from '@/lib/types';

/* ------------------------------------------------------------------ health */

const HEALTH_STYLES: Record<HealthState, { dot: string; text: string; label: string }> = {
  healthy: { dot: 'bg-status-healthy', text: 'text-status-healthy', label: 'HEALTHY' },
  degraded: { dot: 'bg-status-warning', text: 'text-status-warning', label: 'DEGRADED' },
  down: { dot: 'bg-status-error', text: 'text-status-error', label: 'DOWN' },
  unknown: { dot: 'bg-status-neutral', text: 'text-status-neutral', label: 'UNKNOWN' },
};

export function HealthBadge({
  state,
  label,
  className,
}: {
  state: HealthState;
  /** Overrides the default word, e.g. a component name. */
  label?: string;
  className?: string;
}) {
  const style = HEALTH_STYLES[state];
  return (
    <span className={clsx('inline-flex items-center gap-1.5', className)}>
      <span className={clsx('status-dot', style.dot)} aria-hidden="true" />
      <span className={clsx('text-2xs font-medium uppercase tracking-label', style.text)}>
        {label ?? style.label}
      </span>
    </span>
  );
}

/* -------------------------------------------------------------- connection */

const CONNECTION_STYLES: Record<
  ConnectionState,
  { dot: string; text: string; label: string; pulse: boolean }
> = {
  connecting: {
    dot: 'bg-status-info',
    text: 'text-status-info',
    label: 'CONNECTING',
    pulse: true,
  },
  connected: {
    dot: 'bg-status-healthy',
    text: 'text-status-healthy',
    label: 'STREAM CONNECTED',
    pulse: false,
  },
  // Open socket, no recent events. Saying "connected" here would be untrue in
  // the only sense the user cares about.
  stale: { dot: 'bg-status-warning', text: 'text-status-warning', label: 'STALE', pulse: false },
  disconnected: {
    dot: 'bg-status-neutral',
    text: 'text-status-neutral',
    label: 'DISCONNECTED',
    pulse: false,
  },
  error: { dot: 'bg-status-error', text: 'text-status-error', label: 'ERROR', pulse: false },
};

export function ConnectionBadge({
  state,
  className,
}: {
  state: ConnectionState;
  className?: string;
}) {
  const style = CONNECTION_STYLES[state];
  return (
    <span
      className={clsx('inline-flex items-center gap-1.5', className)}
      // Announce transitions, but politely: a connection blip should not
      // interrupt whatever a screen reader is currently saying.
      role="status"
      aria-live="polite"
    >
      <span
        className={clsx('status-dot', style.dot, style.pulse && 'animate-pulse-live')}
        aria-hidden="true"
      />
      <span className={clsx('text-2xs font-medium uppercase tracking-label', style.text)}>
        {style.label}
      </span>
    </span>
  );
}

/* -------------------------------------------------------------------- mode */

const MODE_STYLES: Record<
  StreamMode,
  { dot: string; text: string; border: string; label: string; pulse: boolean }
> = {
  live: {
    dot: 'bg-status-healthy',
    text: 'text-status-healthy',
    border: 'border-status-healthy/30 bg-status-healthy/5',
    label: 'LIVE',
    pulse: true,
  },
  replay: {
    dot: 'bg-status-info',
    text: 'text-status-info',
    border: 'border-status-info/30 bg-status-info/5',
    label: 'REPLAY',
    pulse: false,
  },
  offline: {
    dot: 'bg-status-neutral',
    text: 'text-status-neutral',
    border: 'border-line bg-surface-inset',
    label: 'OFFLINE',
    pulse: false,
  },
};

/**
 * The LIVE / REPLAY / OFFLINE badge.
 *
 * `live` is only ever passed when a socket is genuinely delivering live-sourced
 * events. Replayed data says REPLAY, however real the underlying telemetry is.
 */
export function ModeBadge({ mode, className }: { mode: StreamMode; className?: string }) {
  const style = MODE_STYLES[mode];
  return (
    <span
      className={clsx(
        'inline-flex items-center gap-1.5 rounded border px-2 py-1',
        style.border,
        className,
      )}
      role="status"
      aria-live="polite"
    >
      <span
        className={clsx('status-dot', style.dot, style.pulse && 'animate-pulse-live')}
        aria-hidden="true"
      />
      <span className={clsx('text-2xs font-semibold uppercase tracking-label', style.text)}>
        {style.label}
      </span>
    </span>
  );
}
