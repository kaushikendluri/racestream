/**
 * Global header.
 *
 * Present on every screen and always answering the same five questions, in the
 * order an operator asks them: what mode am I in, what session is this, is the
 * stream connected, how fresh is the data, what time is it.
 *
 * Every value here is passed in from measured state. Nothing in this component
 * invents a status.
 */

import { Logo } from './Logo';
import { ConnectionBadge, ModeBadge } from './StatusIndicator';
import { UtcClock } from './UtcClock';
import { formatAge, NOT_AVAILABLE } from '@/lib/format';
import type { ConnectionState, StreamMode } from '@/lib/types';

export interface SessionHeaderInfo {
  name: string | null;
  circuit: string | null;
  type: string | null;
}

interface HeaderProps {
  mode: StreamMode;
  connection: ConnectionState;
  session: SessionHeaderInfo | null;
  /** Milliseconds since the last event. `null` when nothing has arrived yet. */
  lastEventAgeMs: number | null;
}

export function Header({ mode, connection, session, lastEventAgeMs }: HeaderProps) {
  return (
    <header className="flex h-header shrink-0 items-center gap-4 border-b border-line bg-surface-raised px-4">
      {/* The rail carries the logo on desktop; on mobile the header does. */}
      <Logo className="md:hidden" variant="mark" />

      <ModeBadge mode={mode} />

      {/* Session identity. Truncates rather than wraps: the header must not
       * change height when a circuit name is long. */}
      <div className="flex min-w-0 flex-1 items-center gap-3">
        {session ? (
          <div className="flex min-w-0 items-baseline gap-2.5">
            <h1 className="truncate text-sm font-semibold text-content-primary">
              {session.name ?? NOT_AVAILABLE}
            </h1>
            {session.type && (
              <span className="shrink-0 rounded-sm border border-line bg-surface-inset px-1.5 py-0.5 text-2xs font-medium uppercase tracking-label text-content-secondary">
                {session.type}
              </span>
            )}
            {session.circuit && (
              <span className="hidden truncate text-xs text-content-tertiary lg:inline">
                {session.circuit}
              </span>
            )}
          </div>
        ) : (
          <span className="text-xs text-content-tertiary">No session selected</span>
        )}
      </div>

      {/* Freshness. Shown next to the connection state because the two together
       * are what "is this real right now" actually means. */}
      <div className="hidden flex-col items-end sm:flex">
        <span className="label">LAST EVENT</span>
        <span className="font-mono text-xs text-content-secondary">
          {lastEventAgeMs == null ? NOT_AVAILABLE : formatAge(lastEventAgeMs)}
        </span>
      </div>

      <div className="hidden lg:block">
        <ConnectionBadge state={connection} />
      </div>

      <div className="divider-v hidden h-6 self-center sm:block" />

      <UtcClock />
    </header>
  );
}
