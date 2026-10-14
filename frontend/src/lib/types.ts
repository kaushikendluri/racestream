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

/* ---------------------------------------------------------------- live state
 * Mirrors racestream_common.domain.SessionState.as_dict(). Every numeric field
 * is nullable because the backend reports "not yet known" as null rather than
 * substituting a zero, and the UI must preserve that distinction.
 */

export interface DriverSectors {
  current: [number | null, number | null, number | null];
  last: [number | null, number | null, number | null];
  best: [number | null, number | null, number | null];
}

export interface DriverTelemetry {
  speed: number | null;
  throttle: number | null;
  brake: number | null;
  n_gear: number | null;
  rpm: number | null;
  drs: number | null;
}

export interface LiveDriver {
  driver_number: number;
  position: number | null;
  gap_to_leader: number | null;
  interval: number | null;
  laps_down: number | null;
  current_lap: number | null;
  last_lap_time: number | null;
  best_lap_time: number | null;
  best_lap_number: number | null;
  sectors: DriverSectors;
  telemetry: DriverTelemetry;
  track_position: { x: number | null; y: number | null };
  last_lap_was_personal_best: boolean | null;
  last_update: string | null;
  telemetry_samples: number;
}

export interface LiveSessionState {
  session_id: number;
  session_name: string | null;
  session_type: string | null;
  circuit: string | null;
  current_lap: number | null;
  total_laps: number | null;
  session_best_lap: number | null;
  session_best_lap_driver: number | null;
  session_best_sectors: [number | null, number | null, number | null];
  session_best_sector_drivers: { s1: number | null; s2: number | null; s3: number | null };
  weather: {
    air_temperature: number | null;
    track_temperature: number | null;
    rainfall: number | null;
  };
  last_event_at: string | null;
  events_applied: number;
  /** 'live' | 'replay' | 'backfill' -- drives the LIVE/REPLAY badge honestly. */
  last_source: string | null;
  drivers: LiveDriver[];
}

/* ------------------------------------------------------------- wire messages */

export interface HelloMessage {
  type: 'hello';
  connection_id: number;
  server_time: string;
  active_sessions: number[];
  channels: string[];
  stale_after_s: number;
  disconnected_after_s: number;
}

export interface SnapshotMessage {
  type: 'snapshot';
  sent_at: string;
  state: LiveSessionState;
}

export interface EventMessage {
  type: 'event';
  sent_at: string;
  event: {
    event_id: string;
    event_type: string;
    source: string;
    session_id: number;
    driver_id: number | null;
    timestamp: string;
    payload: Record<string, unknown>;
  };
}

export interface SubscribedMessage {
  type: 'subscribed';
  session_id: number | null;
  channels: string[];
  drivers: number[];
}

export interface ErrorMessage {
  type: 'error';
  error: string;
  detail: string;
}

export interface PongMessage {
  type: 'pong';
  server_time: string;
}

export type ServerMessage =
  | HelloMessage
  | SnapshotMessage
  | EventMessage
  | SubscribedMessage
  | ErrorMessage
  | PongMessage;

/** A race-control message, kept for the event feed and replay markers. */
export interface RaceControlEntry {
  id: string;
  timestamp: string;
  message: string;
  category: string | null;
  flag: string | null;
  lapNumber: number | null;
  driverNumber: number | null;
}
