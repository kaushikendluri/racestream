/**
 * Polls the API health endpoint.
 *
 * Polling is correct here and only here: health is a low-frequency question
 * about the system, not a stream of events. Everything that *is* a stream goes
 * over the WebSocket. See docs/engineering-decisions.md.
 */

import { useCallback, useEffect, useRef, useState } from 'react';
import { api, RaceStreamApiError } from '@/lib/api';
import { config } from '@/lib/config';
import type { HealthResponse, LoadState } from '@/lib/types';

export function useHealth(intervalMs: number = config.healthPollIntervalMs) {
  const [state, setState] = useState<LoadState<HealthResponse>>({ status: 'idle' });
  // Tracks the in-flight request so a poll that overlaps a slow response does
  // not leave two requests racing to set state.
  const inFlight = useRef<AbortController | null>(null);
  const mounted = useRef(true);

  const fetchHealth = useCallback(async () => {
    inFlight.current?.abort();
    const controller = new AbortController();
    inFlight.current = controller;

    // Only show the loading state on the first fetch. A poll that replaces
    // good data with a spinner every ten seconds is worse than no polling.
    setState((prev) => (prev.status === 'success' ? prev : { status: 'loading' }));

    try {
      const data = await api.health({ signal: controller.signal, timeoutMs: 8000 });
      if (!mounted.current || controller.signal.aborted) return;
      setState({ status: 'success', data, fetchedAt: Date.now() });
    } catch (error) {
      if (!mounted.current) return;
      // A cancelled request is not a failure; it means we superseded it.
      if (error instanceof RaceStreamApiError && error.code === 'cancelled') return;
      setState({
        status: 'error',
        error: error instanceof Error ? error.message : 'Health check failed',
      });
    }
  }, []);

  useEffect(() => {
    mounted.current = true;
    void fetchHealth();
    const timer = setInterval(() => void fetchHealth(), intervalMs);

    // Stop polling a tab nobody is looking at, and refresh immediately when it
    // comes back rather than showing a stale reading.
    const onVisibility = () => {
      if (document.visibilityState === 'visible') void fetchHealth();
    };
    document.addEventListener('visibilitychange', onVisibility);

    return () => {
      mounted.current = false;
      clearInterval(timer);
      document.removeEventListener('visibilitychange', onVisibility);
      inFlight.current?.abort();
    };
  }, [fetchHealth, intervalMs]);

  return { state, refetch: fetchHealth };
}
