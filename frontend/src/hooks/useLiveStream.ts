/**
 * The single WebSocket connection to the backend.
 *
 * One socket per browser, not one per chart: browsers cap concurrent
 * connections per origin, and multiplying server-side state by the number of
 * panels on screen would be wasteful for no gain.
 *
 * Three things here are deliberate and worth reading:
 *
 * 1. **Staleness is tracked separately from connectivity.** An open socket
 *    delivering nothing is not "live". The hook moves connected -> stale ->
 *    disconnected on measured gaps since the last *event*, not since the last
 *    frame, because a heartbeat is not data.
 *
 * 2. **Snapshots are stored in a ref, not in state.** At ten snapshots a second
 *    with twenty drivers, calling setState on every frame would re-render the
 *    entire dashboard ten times a second. The ref is written on arrival and
 *    published to React on a fixed interval instead.
 *
 * 3. **Reconnect backs off exponentially with jitter.** Without jitter, every
 *    client that dropped during a backend restart reconnects in lockstep and
 *    hits it again at the same instant.
 */

import { useCallback, useEffect, useRef, useState } from 'react';
import { config } from '@/lib/config';
import type {
  ConnectionState,
  LiveSessionState,
  RaceControlEntry,
  ServerMessage,
  StreamMode,
} from '@/lib/types';

const MAX_RACE_CONTROL_ENTRIES = 50;
const PING_INTERVAL_MS = 20_000;

export interface LiveStreamOptions {
  sessionId: number | null;
  channels?: string[];
  drivers?: number[];
  /** Set false to hold the connection closed, e.g. on a page that does not need it. */
  enabled?: boolean;
}

export interface LiveStreamValue {
  connection: ConnectionState;
  mode: StreamMode;
  state: LiveSessionState | null;
  raceControl: RaceControlEntry[];
  /** Milliseconds since the last event. `null` when nothing has arrived yet. */
  lastEventAgeMs: number | null;
  activeSessions: number[];
  error: string | null;
  reconnectAttempts: number;
  reconnectNow: () => void;
}

export function useLiveStream({
  sessionId,
  channels = ['timing', 'session', 'race_control'],
  drivers,
  enabled = true,
}: LiveStreamOptions): LiveStreamValue {
  const [connection, setConnection] = useState<ConnectionState>('disconnected');
  const [state, setState] = useState<LiveSessionState | null>(null);
  const [raceControl, setRaceControl] = useState<RaceControlEntry[]>([]);
  const [activeSessions, setActiveSessions] = useState<number[]>([]);
  const [error, setError] = useState<string | null>(null);
  const [lastEventAgeMs, setLastEventAgeMs] = useState<number | null>(null);
  const [reconnectAttempts, setReconnectAttempts] = useState(0);

  // Written on every frame, read on a timer. Keeps the firehose out of React.
  const pendingState = useRef<LiveSessionState | null>(null);
  const lastEventAt = useRef<number | null>(null);

  const socket = useRef<WebSocket | null>(null);
  // connect() and scheduleReconnect() are mutually recursive. Routing one
  // through a ref breaks the cycle without a use-before-declaration.
  const scheduleReconnectRef = useRef<() => void>(() => {});
  const reconnectTimer = useRef<ReturnType<typeof setTimeout> | null>(null);
  const pingTimer = useRef<ReturnType<typeof setInterval> | null>(null);
  const attempt = useRef(0);
  // Guards against a reconnect being scheduled after the component unmounts.
  const shouldRun = useRef(enabled);
  // Held in a ref so the connect callback does not need them as dependencies,
  // which would tear down and rebuild the socket on every render.
  const subscription = useRef({ sessionId, channels, drivers });
  subscription.current = { sessionId, channels, drivers };

  /* ------------------------------------------------------- publish to React */

  useEffect(() => {
    const timer = setInterval(() => {
      if (pendingState.current) {
        setState(pendingState.current);
        pendingState.current = null;
      }

      // Recomputed on the same tick so freshness and data never disagree.
      setLastEventAgeMs(lastEventAt.current ? Date.now() - lastEventAt.current : null);
    }, config.renderIntervalMs);

    return () => clearInterval(timer);
  }, []);

  /* ------------------------------------------------- connected -> stale -> gone */

  useEffect(() => {
    const timer = setInterval(() => {
      // Only downgrade a connection we believe is up; a socket that is already
      // closed is handled by the close handler.
      setConnection((current) => {
        if (current !== 'connected' && current !== 'stale') return current;
        if (lastEventAt.current == null) return current;
        const age = Date.now() - lastEventAt.current;
        if (age > config.disconnectedAfterMs) return 'disconnected';
        if (age > config.staleAfterMs) return 'stale';
        return 'connected';
      });
    }, 1000);

    return () => clearInterval(timer);
  }, []);

  /* --------------------------------------------------------------- messages */

  const handleMessage = useCallback((raw: MessageEvent<string | Blob>) => {
    const parse = (text: string) => {
      let message: ServerMessage;
      try {
        message = JSON.parse(text) as ServerMessage;
      } catch {
        // A frame we cannot parse is a bug worth surfacing, not worth dropping
        // the connection over.
        setError('Received an unreadable frame from the server.');
        return;
      }

      switch (message.type) {
        case 'hello':
          setActiveSessions(message.active_sessions);
          setError(null);
          break;

        case 'snapshot':
          pendingState.current = message.state;
          lastEventAt.current = Date.now();
          setConnection('connected');
          break;

        case 'event':
          lastEventAt.current = Date.now();
          setConnection('connected');
          if (message.event.event_type === 'race_control') {
            const payload = message.event.payload as Record<string, unknown>;
            setRaceControl((prev) =>
              [
                {
                  id: message.event.event_id,
                  timestamp: message.event.timestamp,
                  message: String(payload.message ?? ''),
                  category: (payload.category as string | null) ?? null,
                  flag: (payload.flag as string | null) ?? null,
                  lapNumber: (payload.lap_number as number | null) ?? null,
                  driverNumber: message.event.driver_id,
                },
                ...prev,
                // Bounded: a long session would otherwise grow this without limit.
              ].slice(0, MAX_RACE_CONTROL_ENTRIES),
            );
          }
          break;

        case 'error':
          setError(`${message.error}: ${message.detail}`);
          break;

        case 'subscribed':
        case 'pong':
          break;
      }
    };

    if (typeof raw.data === 'string') {
      parse(raw.data);
    } else {
      // The server sends binary frames; decode before parsing.
      void raw.data.text().then(parse);
    }
  }, []);

  /* -------------------------------------------------------------- connection */

  const connect = useCallback(() => {
    if (!shouldRun.current) return;
    if (socket.current && socket.current.readyState <= WebSocket.OPEN) return;

    setConnection('connecting');
    let ws: WebSocket;
    try {
      ws = new WebSocket(config.wsUrl);
    } catch {
      setConnection('error');
      setError('Could not open a WebSocket to the server.');
      scheduleReconnectRef.current();
      return;
    }
    socket.current = ws;

    ws.onopen = () => {
      attempt.current = 0;
      setReconnectAttempts(0);
      setError(null);
      setConnection('connected');

      const { sessionId: id, channels: ch, drivers: drv } = subscription.current;
      ws.send(
        JSON.stringify({
          type: 'subscribe',
          ...(id != null ? { session_id: id } : {}),
          channels: ch,
          ...(drv && drv.length ? { drivers: drv } : {}),
        }),
      );

      // The heartbeat keeps intermediaries from closing an idle connection and
      // gives the server a liveness signal from us.
      pingTimer.current = setInterval(() => {
        if (ws.readyState === WebSocket.OPEN) ws.send(JSON.stringify({ type: 'ping' }));
      }, PING_INTERVAL_MS);
    };

    ws.onmessage = handleMessage;

    ws.onerror = () => {
      // onerror carries no useful detail in browsers; onclose does the work.
      setConnection('error');
    };

    ws.onclose = () => {
      if (pingTimer.current) {
        clearInterval(pingTimer.current);
        pingTimer.current = null;
      }
      socket.current = null;
      setConnection('disconnected');
      scheduleReconnectRef.current();
    };
  }, [handleMessage]);

  const scheduleReconnect = useCallback(() => {
    if (!shouldRun.current) return;
    if (reconnectTimer.current) return;

    attempt.current += 1;
    setReconnectAttempts(attempt.current);

    const { baseDelayMs, maxDelayMs, jitterRatio } = config.reconnect;
    const exponential = Math.min(baseDelayMs * 2 ** (attempt.current - 1), maxDelayMs);
    // Jitter: without it, every client dropped by a backend restart comes back
    // in lockstep and hits it again simultaneously.
    const jitter = exponential * jitterRatio * (Math.random() * 2 - 1);
    const delay = Math.max(baseDelayMs, exponential + jitter);

    reconnectTimer.current = setTimeout(() => {
      reconnectTimer.current = null;
      connect();
    }, delay);
  }, [connect]);

  scheduleReconnectRef.current = scheduleReconnect;

  const reconnectNow = useCallback(() => {
    if (reconnectTimer.current) {
      clearTimeout(reconnectTimer.current);
      reconnectTimer.current = null;
    }
    attempt.current = 0;
    setReconnectAttempts(0);
    socket.current?.close();
    connect();
  }, [connect]);

  useEffect(() => {
    shouldRun.current = enabled;
    if (enabled) {
      connect();
    } else {
      socket.current?.close();
    }

    return () => {
      shouldRun.current = false;
      if (reconnectTimer.current) clearTimeout(reconnectTimer.current);
      if (pingTimer.current) clearInterval(pingTimer.current);
      socket.current?.close();
      socket.current = null;
    };
  }, [enabled, connect]);

  /* Re-subscribe in place when the selection changes, rather than reconnecting. */
  useEffect(() => {
    const ws = socket.current;
    if (!ws || ws.readyState !== WebSocket.OPEN) return;
    ws.send(
      JSON.stringify({
        type: 'subscribe',
        ...(sessionId != null ? { session_id: sessionId } : {}),
        channels,
        ...(drivers && drivers.length ? { drivers } : {}),
      }),
    );
  }, [sessionId, channels, drivers]);

  /**
   * Mode is read off the wire, never inferred.
   *
   * `last_source` is set by the backend from the `source` field of the events
   * that built the state. Replayed telemetry is real data, but it is not live,
   * and a badge that said LIVE over it would be lying about the one thing this
   * UI most needs to be right about. A healthy connection carrying replayed
   * events therefore reads REPLAY, and anything unhealthy reads OFFLINE.
   */
  const mode: StreamMode =
    connection !== 'connected' || state == null
      ? 'offline'
      : state.last_source === 'live'
        ? 'live'
        : state.last_source == null
          ? 'offline'
          : 'replay';

  return {
    connection,
    mode,
    state,
    raceControl,
    lastEventAgeMs,
    activeSessions,
    error,
    reconnectAttempts,
    reconnectNow,
  };
}
