/**
 * Typed API client.
 *
 * One place that knows how to talk to the backend, so error handling, timeouts
 * and abort behaviour are uniform. Every caller gets either data or a typed
 * failure - never a half-parsed response.
 */

import { config } from './config';
import type {
  ApiError,
  DriverInfo,
  HealthResponse,
  LiveSessionState,
  ReadinessResponse,
  SessionSummary,
} from './types';

export class RaceStreamApiError extends Error {
  constructor(
    message: string,
    readonly status: number,
    readonly code: string = 'request_failed',
    readonly path?: string,
  ) {
    super(message);
    this.name = 'RaceStreamApiError';
  }

  /** Whether retrying the same request could plausibly succeed. */
  get isRetryable(): boolean {
    return this.status === 0 || this.status === 408 || this.status >= 500;
  }
}

interface RequestOptions {
  signal?: AbortSignal;
  timeoutMs?: number;
}

const DEFAULT_TIMEOUT_MS = 15_000;

async function request<T>(path: string, options: RequestOptions = {}): Promise<T> {
  const { signal, timeoutMs = DEFAULT_TIMEOUT_MS } = options;

  // Compose the caller's abort signal with our own timeout, so a component
  // unmounting and a slow server both cancel the same request cleanly.
  const timeoutController = new AbortController();
  const timer = setTimeout(() => timeoutController.abort(), timeoutMs);
  const onExternalAbort = () => timeoutController.abort();
  signal?.addEventListener('abort', onExternalAbort);

  try {
    const response = await fetch(`${config.apiBaseUrl}${path}`, {
      signal: timeoutController.signal,
      headers: { Accept: 'application/json' },
    });

    if (!response.ok) {
      // The API returns a structured error body; fall back gracefully if the
      // failure happened before the application layer (a proxy, say).
      let detail = response.statusText;
      let code = 'request_failed';
      try {
        const body = (await response.json()) as Partial<ApiError>;
        detail = body.detail ?? detail;
        code = body.error ?? code;
      } catch {
        /* non-JSON error body; the status text is all we have */
      }
      throw new RaceStreamApiError(detail, response.status, code, path);
    }

    return (await response.json()) as T;
  } catch (error) {
    if (error instanceof RaceStreamApiError) throw error;
    if (error instanceof DOMException && error.name === 'AbortError') {
      // Distinguish "the caller cancelled" from "we gave up waiting": only the
      // latter is a fault worth showing the user.
      if (signal?.aborted) throw new RaceStreamApiError('Request cancelled', 0, 'cancelled', path);
      throw new RaceStreamApiError(
        `Request timed out after ${timeoutMs}ms`,
        408,
        'timeout',
        path,
      );
    }
    throw new RaceStreamApiError(
      error instanceof Error ? error.message : 'Network request failed',
      0,
      'network_error',
      path,
    );
  } finally {
    clearTimeout(timer);
    signal?.removeEventListener('abort', onExternalAbort);
  }
}

export const api = {
  health: (options?: RequestOptions) =>
    request<HealthResponse>('/api/system/health', options),

  readiness: (options?: RequestOptions) =>
    request<ReadinessResponse>('/api/system/ready', options),

  sessions: (params: { year?: number; sessionType?: string } = {}, options?: RequestOptions) => {
    const query = new URLSearchParams();
    if (params.year != null) query.set('year', String(params.year));
    if (params.sessionType) query.set('session_type', params.sessionType);
    const suffix = query.toString();
    return request<SessionSummary[]>(`/api/sessions${suffix ? `?${suffix}` : ''}`, options);
  },

  session: (sessionId: number, options?: RequestOptions) =>
    request<SessionSummary>(`/api/sessions/${sessionId}`, options),

  /** Reference data: fetched once, not pushed on every snapshot. */
  sessionDrivers: (sessionId: number, options?: RequestOptions) =>
    request<DriverInfo[]>(`/api/sessions/${sessionId}/drivers`, options),

  /** The same payload the WebSocket pushes, for an immediate first paint. */
  liveSnapshot: (sessionId: number, options?: RequestOptions) =>
    request<{ sent_at: string; state: LiveSessionState }>(
      `/api/live/${sessionId}`,
      options,
    ),
};
