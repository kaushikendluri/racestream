/**
 * Formatting for machine-generated values.
 *
 * The rule every function here follows: a value that is `null` or `undefined`
 * renders as the em-dash placeholder, never as `0`, `0.00` or `--:--`. A zero
 * is a measurement; a dash is the absence of one, and the two must never look
 * alike on a screen someone is making decisions from.
 */

export const NOT_AVAILABLE = '—'; // em dash

/** `91.204` -> `1:31.204`. Lap and sector times. */
export function formatLapTime(seconds: number | null | undefined): string {
  if (seconds == null || !Number.isFinite(seconds) || seconds <= 0) return NOT_AVAILABLE;
  const minutes = Math.floor(seconds / 60);
  const rest = seconds - minutes * 60;
  if (minutes === 0) return rest.toFixed(3);
  return `${minutes}:${rest.toFixed(3).padStart(6, '0')}`;
}

/** A gap or interval, always signed so the direction is unambiguous. */
export function formatGap(
  seconds: number | null | undefined,
  lapsDown?: number | null,
): string {
  if (lapsDown != null && lapsDown > 0) return `+${lapsDown} LAP${lapsDown > 1 ? 'S' : ''}`;
  if (seconds == null || !Number.isFinite(seconds)) return NOT_AVAILABLE;
  if (seconds === 0) return 'LEADER';
  return `${seconds > 0 ? '+' : ''}${seconds.toFixed(3)}`;
}

/** A delta against a reference time. Sign is meaningful, so it is always shown. */
export function formatDelta(seconds: number | null | undefined): string {
  if (seconds == null || !Number.isFinite(seconds)) return NOT_AVAILABLE;
  return `${seconds >= 0 ? '+' : ''}${seconds.toFixed(3)}`;
}

export function formatSector(seconds: number | null | undefined): string {
  if (seconds == null || !Number.isFinite(seconds) || seconds <= 0) return NOT_AVAILABLE;
  return seconds.toFixed(3);
}

/** Integer telemetry channels: speed, RPM, gear. */
export function formatInt(value: number | null | undefined): string {
  if (value == null || !Number.isFinite(value)) return NOT_AVAILABLE;
  return Math.round(value).toString();
}

export function formatPercent(value: number | null | undefined): string {
  if (value == null || !Number.isFinite(value)) return NOT_AVAILABLE;
  return `${Math.round(value)}`;
}

export function formatTemperature(value: number | null | undefined): string {
  if (value == null || !Number.isFinite(value)) return NOT_AVAILABLE;
  return value.toFixed(1);
}

/**
 * Latency, with the unit chosen by magnitude so the number stays short enough
 * to scan. Sub-millisecond figures keep a decimal rather than rounding to 0.
 */
export function formatLatency(ms: number | null | undefined): string {
  if (ms == null || !Number.isFinite(ms)) return NOT_AVAILABLE;
  if (ms < 1) return `${ms.toFixed(2)}ms`;
  if (ms < 1000) return `${ms.toFixed(ms < 10 ? 1 : 0)}ms`;
  return `${(ms / 1000).toFixed(2)}s`;
}

/** Throughput. Thousands are abbreviated; the precision is not real above 1k. */
export function formatRate(perSecond: number | null | undefined): string {
  if (perSecond == null || !Number.isFinite(perSecond)) return NOT_AVAILABLE;
  if (perSecond < 1000) return perSecond.toFixed(perSecond < 10 ? 1 : 0);
  return `${(perSecond / 1000).toFixed(1)}k`;
}

export function formatCount(value: number | null | undefined): string {
  if (value == null || !Number.isFinite(value)) return NOT_AVAILABLE;
  return value.toLocaleString('en-US');
}

/** `2026-09-20T05:42:16.247Z` -> `05:42:16` in UTC. */
export function formatClock(date: Date | string | null | undefined): string {
  if (date == null) return NOT_AVAILABLE;
  const d = typeof date === 'string' ? new Date(date) : date;
  if (Number.isNaN(d.getTime())) return NOT_AVAILABLE;
  return d.toISOString().slice(11, 19);
}

/** Same, with milliseconds: telemetry cursors need the sub-second detail. */
export function formatClockMs(date: Date | string | null | undefined): string {
  if (date == null) return NOT_AVAILABLE;
  const d = typeof date === 'string' ? new Date(date) : date;
  if (Number.isNaN(d.getTime())) return NOT_AVAILABLE;
  return d.toISOString().slice(11, 23);
}

export function formatDate(date: string | null | undefined): string {
  if (!date) return NOT_AVAILABLE;
  const d = new Date(date);
  if (Number.isNaN(d.getTime())) return NOT_AVAILABLE;
  return d.toLocaleDateString('en-GB', {
    day: '2-digit',
    month: 'short',
    year: 'numeric',
    timeZone: 'UTC',
  });
}

/** "182ms ago" / "12.4s ago". Backs the data-freshness indicator. */
export function formatAge(ms: number | null | undefined): string {
  if (ms == null || !Number.isFinite(ms) || ms < 0) return NOT_AVAILABLE;
  if (ms < 1000) return `${Math.round(ms)}ms ago`;
  if (ms < 60_000) return `${(ms / 1000).toFixed(1)}s ago`;
  const minutes = Math.floor(ms / 60_000);
  return `${minutes}m ${Math.round((ms % 60_000) / 1000)}s ago`;
}

export function formatDuration(seconds: number | null | undefined): string {
  if (seconds == null || !Number.isFinite(seconds)) return NOT_AVAILABLE;
  const h = Math.floor(seconds / 3600);
  const m = Math.floor((seconds % 3600) / 60);
  const s = Math.floor(seconds % 60);
  if (h > 0) return `${h}h ${m}m`;
  if (m > 0) return `${m}m ${s}s`;
  return `${s}s`;
}

/**
 * Team colours arrive from upstream as a bare hex triplet with no `#`, and are
 * sometimes absent. Normalising in one place keeps that quirk out of components.
 */
export function teamColour(colour: string | null | undefined, fallback = '#6B7483'): string {
  if (!colour) return fallback;
  const trimmed = colour.trim().replace(/^#/, '');
  return /^[0-9a-fA-F]{6}$/.test(trimmed) ? `#${trimmed}` : fallback;
}
