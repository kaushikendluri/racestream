/**
 * Application frame: navigation rail, header, routed content.
 *
 * Owns the layout and the one piece of genuinely global UI state (rail
 * collapsed). Stream state is deliberately *not* owned here yet - it arrives in
 * the WebSocket phase and will be provided by a context, so that panels
 * subscribe to what they need instead of the shell re-rendering on every event.
 */

import { useCallback, useEffect, useState } from 'react';
import { Outlet } from 'react-router-dom';
import { Header, type SessionHeaderInfo } from './Header';
import { MobileNav, NavRail } from './NavRail';
import { useHealth } from '@/hooks/useHealth';
import type { ConnectionState, StreamMode } from '@/lib/types';

const RAIL_STORAGE_KEY = 'racestream.rail.collapsed';

export function AppShell() {
  const [collapsed, setCollapsed] = useState<boolean>(() => {
    // A per-viewer convenience, so localStorage is the right home for it.
    // Wrapped because it throws in private mode and returns null when cleared.
    try {
      return window.localStorage.getItem(RAIL_STORAGE_KEY) === 'true';
    } catch {
      return false;
    }
  });

  const toggleCollapsed = useCallback(() => {
    setCollapsed((prev) => {
      const next = !prev;
      try {
        window.localStorage.setItem(RAIL_STORAGE_KEY, String(next));
      } catch {
        /* storage unavailable; the rail still toggles for this session */
      }
      return next;
    });
  }, []);

  // Tablet widths get the icon rail automatically, but a deliberate choice by
  // the user is not overridden once made.
  const [autoCollapsed, setAutoCollapsed] = useState(false);
  useEffect(() => {
    const query = window.matchMedia('(max-width: 1279px)');
    const apply = () => setAutoCollapsed(query.matches);
    apply();
    query.addEventListener('change', apply);
    return () => query.removeEventListener('change', apply);
  }, []);

  const { state: healthState } = useHealth();
  const environment =
    healthState.status === 'success' ? healthState.data.environment : 'unknown';

  /*
   * Until the WebSocket lands, the shell reports exactly what is true: nothing
   * is streaming, so the mode is OFFLINE, the connection is disconnected, and
   * there is no last-event age. These are wired to real state in Phase 5 -
   * they are not placeholders standing in for a value we could have shown.
   */
  const mode: StreamMode = 'offline';
  const connection: ConnectionState = 'disconnected';
  const session: SessionHeaderInfo | null = null;
  const lastEventAgeMs: number | null = null;

  return (
    <div className="flex h-screen w-full overflow-hidden bg-surface-base">
      <NavRail
        collapsed={collapsed || autoCollapsed}
        connection={connection}
        environment={environment}
        onToggleCollapsed={toggleCollapsed}
      />

      <div className="flex min-w-0 flex-1 flex-col">
        <Header
          mode={mode}
          connection={connection}
          session={session}
          lastEventAgeMs={lastEventAgeMs}
        />

        {/* Scroll lives on the content region, not the page, so the header and
         * rail stay fixed while a dense panel scrolls under them. */}
        <main className="min-h-0 flex-1 overflow-y-auto overflow-x-hidden pb-16 md:pb-0">
          <Outlet />
        </main>
      </div>

      <MobileNav />
    </div>
  );
}
