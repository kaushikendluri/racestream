-- ===========================================================================
-- RaceStream schema.
--
-- Two tiers, deliberately separated:
--
--   Metadata tier  (sessions, drivers, stints, race_control_events)
--     Small, mutable, relational, queried by joins. Ordinary Postgres tables.
--
--   Telemetry tier (car_telemetry, positions, timing, laps, weather)
--     Append-heavy, time-ordered, never updated in place after the fact.
--     TimescaleDB hypertables, chunked by time.
--
-- Index policy: indexes are added for query patterns that actually exist in
-- services/api, and each one names its query below. High-frequency tables pay
-- for every index on write, so there are deliberately few.
-- ===========================================================================

CREATE EXTENSION IF NOT EXISTS timescaledb;

-- ---------------------------------------------------------------------------
-- Metadata tier
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS sessions (
    session_id          INTEGER PRIMARY KEY,           -- OpenF1 session_key
    meeting_id          INTEGER,                       -- OpenF1 meeting_key
    year                INTEGER,
    session_name        TEXT        NOT NULL,          -- 'Race', 'Qualifying', ...
    session_type        TEXT        NOT NULL,
    circuit_short_name  TEXT,
    country_name        TEXT,
    location            TEXT,
    date_start          TIMESTAMPTZ,
    date_end            TIMESTAMPTZ,
    gmt_offset          TEXT,
    total_laps          INTEGER,
    -- Populated by the ingestion service once it knows what upstream actually
    -- returned, so the UI can say "telemetry unavailable" from fact.
    has_car_telemetry   BOOLEAN     NOT NULL DEFAULT FALSE,
    has_position        BOOLEAN     NOT NULL DEFAULT FALSE,
    has_laps            BOOLEAN     NOT NULL DEFAULT FALSE,
    has_weather         BOOLEAN     NOT NULL DEFAULT FALSE,
    has_race_control    BOOLEAN     NOT NULL DEFAULT FALSE,
    ingested_at         TIMESTAMPTZ,
    ingest_status       TEXT        NOT NULL DEFAULT 'pending',
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT sessions_ingest_status_valid
        CHECK (ingest_status IN ('pending', 'running', 'complete', 'partial', 'failed'))
);

-- Session Explorer lists by season then date. One composite index serves both.
CREATE INDEX IF NOT EXISTS sessions_year_date_idx
    ON sessions (year DESC, date_start DESC);

CREATE TABLE IF NOT EXISTS teams (
    team_name    TEXT PRIMARY KEY,
    team_colour  TEXT                                  -- hex, no '#', from OpenF1
);

CREATE TABLE IF NOT EXISTS drivers (
    session_id       INTEGER NOT NULL REFERENCES sessions(session_id) ON DELETE CASCADE,
    driver_number    INTEGER NOT NULL,
    name_acronym     TEXT,                             -- 'VER' - the timing tower label
    full_name        TEXT,
    first_name       TEXT,
    last_name        TEXT,
    broadcast_name   TEXT,
    country_code     TEXT,
    team_name        TEXT,
    team_colour      TEXT,
    headshot_url     TEXT,
    -- A driver's entry is per session: numbers and teams change between them.
    PRIMARY KEY (session_id, driver_number)
);

CREATE TABLE IF NOT EXISTS stints (
    session_id          INTEGER NOT NULL REFERENCES sessions(session_id) ON DELETE CASCADE,
    driver_number       INTEGER NOT NULL,
    stint_number        INTEGER NOT NULL,
    compound            TEXT,
    lap_start           INTEGER,
    lap_end             INTEGER,
    tyre_age_at_start   INTEGER,
    PRIMARY KEY (session_id, driver_number, stint_number)
);

CREATE TABLE IF NOT EXISTS race_control_events (
    session_id     INTEGER     NOT NULL REFERENCES sessions(session_id) ON DELETE CASCADE,
    event_time     TIMESTAMPTZ NOT NULL,
    driver_number  INTEGER,
    category       TEXT,
    flag           TEXT,
    scope          TEXT,
    sector         INTEGER,
    lap_number     INTEGER,
    message        TEXT        NOT NULL,
    -- Natural key: upstream has no message id, and the same message text can
    -- legitimately repeat later in a session. See docs/data-contracts.md.
    PRIMARY KEY (session_id, event_time, message)
);

-- Replay timeline markers: "all race-control events for this session, in order".
CREATE INDEX IF NOT EXISTS race_control_session_time_idx
    ON race_control_events (session_id, event_time);

-- ---------------------------------------------------------------------------
-- Telemetry tier - hypertables
--
-- Every telemetry table carries (session_id, driver_number, sample_time) as its
-- natural key. That triple is the idempotency contract: replaying the same
-- source rows produces the same primary keys, so ON CONFLICT DO NOTHING makes
-- reprocessing a no-op. event_id is NOT used for this - it is a fresh UUID on
-- every ingestion run and would therefore not deduplicate across runs.
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS car_telemetry (
    session_id     INTEGER     NOT NULL,
    driver_number  INTEGER     NOT NULL,
    sample_time    TIMESTAMPTZ NOT NULL,
    speed          SMALLINT,
    throttle       SMALLINT,
    brake          SMALLINT,
    n_gear         SMALLINT,
    rpm            INTEGER,
    drs            SMALLINT,
    source         TEXT        NOT NULL DEFAULT 'live',
    ingested_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (session_id, driver_number, sample_time)
);

SELECT create_hypertable(
    'car_telemetry', 'sample_time',
    chunk_time_interval => INTERVAL '1 hour',
    if_not_exists       => TRUE
);

CREATE TABLE IF NOT EXISTS positions (
    session_id     INTEGER     NOT NULL,
    driver_number  INTEGER     NOT NULL,
    sample_time    TIMESTAMPTZ NOT NULL,
    x              DOUBLE PRECISION NOT NULL,
    y              DOUBLE PRECISION NOT NULL,
    z              DOUBLE PRECISION,
    source         TEXT        NOT NULL DEFAULT 'live',
    ingested_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (session_id, driver_number, sample_time)
);

SELECT create_hypertable(
    'positions', 'sample_time',
    chunk_time_interval => INTERVAL '1 hour',
    if_not_exists       => TRUE
);

CREATE TABLE IF NOT EXISTS timing (
    session_id      INTEGER     NOT NULL,
    driver_number   INTEGER     NOT NULL,
    sample_time     TIMESTAMPTZ NOT NULL,
    position        SMALLINT,
    gap_to_leader   DOUBLE PRECISION,
    interval_ahead  DOUBLE PRECISION,
    laps_down       SMALLINT,
    source          TEXT        NOT NULL DEFAULT 'live',
    PRIMARY KEY (session_id, driver_number, sample_time)
);

SELECT create_hypertable(
    'timing', 'sample_time',
    chunk_time_interval => INTERVAL '6 hours',
    if_not_exists       => TRUE
);

CREATE TABLE IF NOT EXISTS laps (
    session_id         INTEGER     NOT NULL,
    driver_number      INTEGER     NOT NULL,
    lap_number         INTEGER     NOT NULL,
    lap_start          TIMESTAMPTZ NOT NULL,
    lap_duration       DOUBLE PRECISION,
    duration_sector_1  DOUBLE PRECISION,
    duration_sector_2  DOUBLE PRECISION,
    duration_sector_3  DOUBLE PRECISION,
    i1_speed           SMALLINT,
    i2_speed           SMALLINT,
    st_speed           SMALLINT,
    is_pit_out_lap     BOOLEAN     NOT NULL DEFAULT FALSE,
    -- Derived by the stream processor, not upstream. NULL means "not yet known",
    -- which the UI renders as blank rather than as a zero delta.
    is_personal_best   BOOLEAN,
    is_session_best    BOOLEAN,
    source             TEXT        NOT NULL DEFAULT 'live',
    PRIMARY KEY (session_id, driver_number, lap_number, lap_start)
);

SELECT create_hypertable(
    'laps', 'lap_start',
    chunk_time_interval => INTERVAL '6 hours',
    if_not_exists       => TRUE
);

-- "Lap times for driver X in session Y, in lap order" - the driver page and
-- the analytics lap-evolution chart.
CREATE INDEX IF NOT EXISTS laps_session_driver_lap_idx
    ON laps (session_id, driver_number, lap_number);

CREATE TABLE IF NOT EXISTS weather (
    session_id        INTEGER     NOT NULL,
    sample_time       TIMESTAMPTZ NOT NULL,
    air_temperature   DOUBLE PRECISION,
    track_temperature DOUBLE PRECISION,
    humidity          DOUBLE PRECISION,
    pressure          DOUBLE PRECISION,
    rainfall          SMALLINT,
    wind_speed        DOUBLE PRECISION,
    wind_direction    SMALLINT,
    source            TEXT        NOT NULL DEFAULT 'live',
    PRIMARY KEY (session_id, sample_time)
);

SELECT create_hypertable(
    'weather', 'sample_time',
    chunk_time_interval => INTERVAL '1 day',
    if_not_exists       => TRUE
);

-- ---------------------------------------------------------------------------
-- Pipeline observability
--
-- Latency samples are persisted, not only exposed as Prometheus histograms, so
-- the benchmark page can compute exact percentiles over a named run rather than
-- interpolating from bucket boundaries.
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS pipeline_events (
    observed_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    session_id         INTEGER,
    event_type         TEXT        NOT NULL,
    source             TEXT        NOT NULL,
    run_label          TEXT,                            -- benchmark run tag, nullable
    source_to_ingest_s DOUBLE PRECISION,
    ingest_to_produce_s DOUBLE PRECISION,
    produce_to_consume_s DOUBLE PRECISION,
    consume_to_process_s DOUBLE PRECISION,
    process_to_db_s    DOUBLE PRECISION,
    end_to_end_s       DOUBLE PRECISION
);

SELECT create_hypertable(
    'pipeline_events', 'observed_at',
    chunk_time_interval => INTERVAL '1 hour',
    if_not_exists       => TRUE
);

-- Benchmark percentile queries filter by run then scan. One index, one purpose.
CREATE INDEX IF NOT EXISTS pipeline_events_run_idx
    ON pipeline_events (run_label, observed_at DESC)
    WHERE run_label IS NOT NULL;

-- ---------------------------------------------------------------------------
-- Compression.
--
-- Telemetry chunks are immutable once the session is over, and columnar
-- compression on a segment-by of (session_id, driver_number) typically gives a
-- large reduction on this shape of data. Applied to closed chunks only, so it
-- never touches the chunk being written to.
-- ---------------------------------------------------------------------------

ALTER TABLE car_telemetry SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'session_id, driver_number',
    timescaledb.compress_orderby   = 'sample_time DESC'
);

ALTER TABLE positions SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'session_id, driver_number',
    timescaledb.compress_orderby   = 'sample_time DESC'
);

SELECT add_compression_policy('car_telemetry', INTERVAL '7 days', if_not_exists => TRUE);
SELECT add_compression_policy('positions',     INTERVAL '7 days', if_not_exists => TRUE);
