/**
 * Types mirroring the API's Pydantic response models.
 *
 * Kept hand-written rather than generated so the frontend can be read on its
 * own. The contract test in src/test/contract.test.ts fetches the live OpenAPI
 * schema and fails if these drift, which is the part that actually matters.
 *
 * Convention: `null` means "not measured / not available" and must render as
 * N/A. It never silently becomes 0.
 */

export type HealthState = 'healthy' | 'degraded' | 'down' | 'unknown';

export interface ComponentHealth {
  name: string;
  state: HealthState;
  detail: string | null;
  latency_ms: number | null;
  checked_at: string;
}

export interface HealthResponse {
  state: HealthState;
  version: string;
  environment: string;
  uptime_seconds: number;
  components: ComponentHealth[];
}

export interface ReadinessResponse {
  ready: boolean;
  checks: Record<string, boolean>;
  detail: string | null;
}

export interface SessionSummary {
  session_id: number;
  year: number | null;
  session_name: string;
  session_type: string;
  circuit_short_name: string | null;
  country_name: string | null;
  location: string | null;
  date_start: string | null;
  date_end: string | null;
  total_laps: number | null;
  ingest_status: string;
  ingested_at: string | null;
  available_channels: Record<string, boolean>;
  replay_available: boolean;
}

export interface DriverInfo {
  driver_number: number;
  name_acronym: string | null;
  full_name: string | null;
  broadcast_name: string | null;
  country_code: string | null;
  team_name: string | null;
  team_colour: string | null;
  headshot_url: string | null;
}

export interface ApiError {
  error: string;
  detail: string;
  status_code: number;
  path: string | null;
}

/**
 * How the UI describes its own relationship to the data stream.
 *
 * `stale` is a distinct state on purpose: a socket can be open while no events
 * are arriving, and showing LIVE in that situation would be a lie. The
 * thresholds are configured in one place (lib/config.ts) and shared with the
 * backend's own definition.
 */
export type ConnectionState =
  | 'connecting'
  | 'connected'
  | 'stale'
  | 'disconnected'
  | 'error';

/** Where the data on screen came from. Drives the LIVE / REPLAY / OFFLINE badge. */
export type StreamMode = 'live' | 'replay' | 'offline';

/** Generic fetch lifecycle, so every panel handles the same five cases. */
export type LoadState<T> =
  | { status: 'idle' }
  | { status: 'loading' }
  | { status: 'success'; data: T; fetchedAt: number }
  | { status: 'empty'; reason: string }
  | { status: 'error'; error: string };
