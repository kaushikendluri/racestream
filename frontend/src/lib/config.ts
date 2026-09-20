/**
 * Runtime configuration.
 *
 * Vite only exposes variables prefixed VITE_ to the browser, which is the
 * boundary that keeps server-side configuration out of the bundle. Defaults
 * point at the dev proxy so a clone runs with no .env at all.
 */

export const config = {
  apiBaseUrl: import.meta.env.VITE_API_BASE_URL ?? '',
  wsUrl:
    import.meta.env.VITE_WS_URL ??
    `${window.location.protocol === 'https:' ? 'wss' : 'ws'}://${window.location.host}/ws/live`,

  /**
   * Freshness thresholds, mirroring the API's own values so the two agree on
   * what LIVE means. Measured from the last event received, not from the last
   * socket frame: a heartbeat is not data.
   */
  staleAfterMs: 5_000,
  disconnectedAfterMs: 15_000,

  /** Reconnect backoff: exponential, jittered, capped. */
  reconnect: {
    baseDelayMs: 500,
    maxDelayMs: 15_000,
    jitterRatio: 0.3,
  },

  /**
   * How much telemetry the browser keeps per driver. A race session is millions
   * of samples; holding them all would exhaust memory for no benefit, since the
   * live charts only ever show a moving window. Anything older is a query.
   */
  telemetryWindowSeconds: 120,
  maxSamplesPerChannel: 900,

  /** UI repaint budget. Events arrive faster than a screen can usefully change. */
  renderIntervalMs: 100,

  healthPollIntervalMs: 10_000,
} as const;
