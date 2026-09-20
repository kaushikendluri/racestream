"""Batched, idempotent persistence of events.

Idempotency
-----------
Every statement here is ``ON CONFLICT ... DO NOTHING`` (or ``DO UPDATE`` where a
later value legitimately supersedes an earlier one) against the natural key
``(session_id, driver_number, sample_time)``. That key is derived from the
upstream data, not generated at ingest, so re-running ingestion or replaying a
session writes the same keys and the second pass is a no-op. This is what makes
the consumer's at-least-once delivery behave as effectively-once end to end.

Batching
--------
Rows are accumulated and flushed when either the batch size or the batch timeout
is reached, whichever comes first. The timeout matters: without it, a quiet
session would leave the last few rows unwritten indefinitely.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from datetime import datetime

from racestream_common.config import DatabaseSettings
from racestream_common.db.pool import Database
from racestream_common.obs import METRICS, get_logger
from racestream_common.obs.metrics import utcnow
from racestream_common.schemas import Event, EventType

log = get_logger(__name__)


# ---------------------------------------------------------------------------
# Statements. One per table, each naming its conflict target explicitly.
# ---------------------------------------------------------------------------

_INSERT_CAR_TELEMETRY = """
    INSERT INTO car_telemetry
        (session_id, driver_number, sample_time, speed, throttle, brake,
         n_gear, rpm, drs, source)
    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
    ON CONFLICT (session_id, driver_number, sample_time) DO NOTHING
"""

_INSERT_POSITION = """
    INSERT INTO positions
        (session_id, driver_number, sample_time, x, y, z, source)
    VALUES ($1, $2, $3, $4, $5, $6, $7)
    ON CONFLICT (session_id, driver_number, sample_time) DO NOTHING
"""

_INSERT_TIMING = """
    INSERT INTO timing
        (session_id, driver_number, sample_time, position, gap_to_leader,
         interval_ahead, laps_down, source)
    VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
    ON CONFLICT (session_id, driver_number, sample_time) DO NOTHING
"""

# Laps are the one case where a later write should win: upstream publishes a lap
# row as soon as the lap starts and fills in sector times and the total duration
# afterwards. COALESCE keeps whichever value is known, so a late-arriving NULL
# never erases a figure we already have.
_INSERT_LAP = """
    INSERT INTO laps
        (session_id, driver_number, lap_number, lap_start, lap_duration,
         duration_sector_1, duration_sector_2, duration_sector_3,
         i1_speed, i2_speed, st_speed, is_pit_out_lap, source)
    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
    ON CONFLICT (session_id, driver_number, lap_number, lap_start) DO UPDATE SET
        lap_duration      = COALESCE(EXCLUDED.lap_duration,      laps.lap_duration),
        duration_sector_1 = COALESCE(EXCLUDED.duration_sector_1, laps.duration_sector_1),
        duration_sector_2 = COALESCE(EXCLUDED.duration_sector_2, laps.duration_sector_2),
        duration_sector_3 = COALESCE(EXCLUDED.duration_sector_3, laps.duration_sector_3),
        i1_speed          = COALESCE(EXCLUDED.i1_speed,          laps.i1_speed),
        i2_speed          = COALESCE(EXCLUDED.i2_speed,          laps.i2_speed),
        st_speed          = COALESCE(EXCLUDED.st_speed,          laps.st_speed),
        is_pit_out_lap    = laps.is_pit_out_lap OR EXCLUDED.is_pit_out_lap
"""

_INSERT_WEATHER = """
    INSERT INTO weather
        (session_id, sample_time, air_temperature, track_temperature, humidity,
         pressure, rainfall, wind_speed, wind_direction, source)
    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
    ON CONFLICT (session_id, sample_time) DO NOTHING
"""

_INSERT_RACE_CONTROL = """
    INSERT INTO race_control_events
        (session_id, event_time, driver_number, category, flag, scope,
         sector, lap_number, message)
    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
    ON CONFLICT (session_id, event_time, message) DO NOTHING
"""

_INSERT_PIPELINE_EVENT = """
    INSERT INTO pipeline_events
        (observed_at, session_id, event_type, source, run_label,
         source_to_ingest_s, ingest_to_produce_s, produce_to_consume_s,
         consume_to_process_s, process_to_db_s, end_to_end_s)
    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
"""

_TABLE_FOR_TYPE: dict[EventType, tuple[str, str]] = {
    EventType.CAR_TELEMETRY: ("car_telemetry", _INSERT_CAR_TELEMETRY),
    EventType.POSITION: ("positions", _INSERT_POSITION),
    EventType.TIMING: ("timing", _INSERT_TIMING),
    EventType.LAP: ("laps", _INSERT_LAP),
    EventType.WEATHER: ("weather", _INSERT_WEATHER),
    EventType.RACE_CONTROL: ("race_control_events", _INSERT_RACE_CONTROL),
}


def _source_of(event: Event) -> str:
    return event.source.value if hasattr(event.source, "value") else str(event.source)


def _row_for(event: Event) -> tuple | None:
    """Flatten one event into positional arguments for its statement."""
    p = event.payload
    src = _source_of(event)
    ts: datetime = event.timestamp

    if event.event_type is EventType.CAR_TELEMETRY:
        return (
            event.session_id, event.driver_id, ts, p.speed, p.throttle, p.brake,
            p.n_gear, p.rpm, p.drs, src,
        )
    if event.event_type is EventType.POSITION:
        return (event.session_id, event.driver_id, ts, p.x, p.y, p.z, src)
    if event.event_type is EventType.TIMING:
        return (
            event.session_id, event.driver_id, ts, p.position, p.gap_to_leader,
            p.interval, p.laps_down, src,
        )
    if event.event_type is EventType.LAP:
        # lap_start is part of the primary key, so it must never be NULL. The
        # event timestamp is the fallback and is the same value upstream uses.
        return (
            event.session_id, event.driver_id, p.lap_number, p.date_start or ts,
            p.lap_duration, p.duration_sector_1, p.duration_sector_2,
            p.duration_sector_3, p.i1_speed, p.i2_speed, p.st_speed,
            p.is_pit_out_lap, src,
        )
    if event.event_type is EventType.WEATHER:
        return (
            event.session_id, ts, p.air_temperature, p.track_temperature,
            p.humidity, p.pressure, p.rainfall, p.wind_speed, p.wind_direction, src,
        )
    if event.event_type is EventType.RACE_CONTROL:
        return (
            event.session_id, ts, event.driver_id, p.category, p.flag, p.scope,
            p.sector, p.lap_number, p.message,
        )
    return None  # SESSION_META is handled by the metadata writer, not here.


@dataclass
class WriteStats:
    rows_written: int = 0
    batches: int = 0
    flush_errors: int = 0
    last_flush_at: str | None = None
    last_error: str | None = None
    pending: int = 0
    rows_by_table: dict[str, int] = field(default_factory=dict)

    def as_dict(self) -> dict:
        return {
            "rows_written": self.rows_written,
            "batches": self.batches,
            "flush_errors": self.flush_errors,
            "last_flush_at": self.last_flush_at,
            "last_error": self.last_error,
            "pending": self.pending,
            "rows_by_table": dict(self.rows_by_table),
        }


class TelemetryWriter:
    """Accumulates events and flushes them to TimescaleDB in batches."""

    def __init__(
        self,
        database: Database,
        settings: DatabaseSettings,
        run_label: str | None = None,
        latency_sample_rate: int = 20,
    ) -> None:
        self._db = database
        self._settings = settings
        self._run_label = run_label
        # Persisting a latency row for every event would roughly double the
        # write volume, so a fixed fraction is sampled. Prometheus histograms
        # still see every event; this table exists for exact percentiles over a
        # named benchmark run.
        self._latency_sample_rate = max(1, latency_sample_rate)
        self._seen = 0

        self._buffers: dict[EventType, list[tuple]] = {}
        self._latency_rows: list[tuple] = []
        self._lock = asyncio.Lock()
        self._flush_task: asyncio.Task | None = None
        self._stopping = False
        self.stats = WriteStats()

    async def start(self) -> None:
        self._stopping = False
        self._flush_task = asyncio.create_task(self._periodic_flush(), name="db-flush")

    async def stop(self) -> None:
        self._stopping = True
        if self._flush_task is not None:
            self._flush_task.cancel()
            try:
                await self._flush_task
            except asyncio.CancelledError:
                pass
            self._flush_task = None
        await self.flush()  # never lose the tail of a session

    async def add(self, events: list[Event]) -> None:
        """Buffer a batch, flushing if it pushes us over the size threshold."""
        async with self._lock:
            for event in events:
                row = _row_for(event)
                if row is None:
                    continue
                self._buffers.setdefault(event.event_type, []).append(row)
                self._record_latency(event)
            self.stats.pending = sum(len(v) for v in self._buffers.values())
            METRICS.queue_depth.labels(queue="db_writer").set(self.stats.pending)
            should_flush = self.stats.pending >= self._settings.write_batch_size

        if should_flush:
            await self.flush()

    def _record_latency(self, event: Event) -> None:
        self._seen += 1
        if self._seen % self._latency_sample_rate:
            return
        t = event.trace
        self._latency_rows.append(
            (
                utcnow(),
                event.session_id,
                event.event_type.value,
                _source_of(event),
                self._run_label,
                t.elapsed_seconds("source_ts", "ingest_ts"),
                t.elapsed_seconds("ingest_ts", "produce_ts"),
                t.elapsed_seconds("produce_ts", "consume_ts"),
                t.elapsed_seconds("consume_ts", "process_ts"),
                t.elapsed_seconds("process_ts", "db_ts"),
                t.elapsed_seconds("ingest_ts", "db_ts"),
            )
        )

    async def flush(self) -> int:
        """Write everything buffered. Returns the number of rows written.

        Each table is flushed independently so that a failure on one does not
        discard the others. A failed batch is dropped rather than retried
        forever: Kafka still holds the offsets, so the events come back if the
        consumer has not committed, and an unbounded in-memory retry queue is
        exactly the failure mode this design is trying to avoid.
        """
        async with self._lock:
            buffers, self._buffers = self._buffers, {}
            latency_rows, self._latency_rows = self._latency_rows, []
            self.stats.pending = 0
            METRICS.queue_depth.labels(queue="db_writer").set(0)

        if not buffers and not latency_rows:
            return 0

        written = 0
        loop = asyncio.get_running_loop()

        for event_type, rows in buffers.items():
            if not rows:
                continue
            table, statement = _TABLE_FOR_TYPE[event_type]
            started = loop.time()
            try:
                await self._db.executemany(statement, rows)
                elapsed = loop.time() - started
                METRICS.db_write_latency.labels(table=table).observe(elapsed)
                METRICS.rows_written.labels(table=table).inc(len(rows))
                self.stats.rows_by_table[table] = (
                    self.stats.rows_by_table.get(table, 0) + len(rows)
                )
                written += len(rows)
            except Exception as exc:
                self.stats.flush_errors += 1
                self.stats.last_error = f"{type(exc).__name__}: {exc}"
                METRICS.errors.labels(component="db_writer", kind=type(exc).__name__).inc()
                METRICS.messages_dropped.labels(reason="db_write_failed", stage="persist").inc(
                    len(rows)
                )
                log.error("db.flush_failed", table=table, rows=len(rows), error=str(exc))

        if latency_rows:
            try:
                await self._db.executemany(_INSERT_PIPELINE_EVENT, latency_rows)
            except Exception as exc:
                # Diagnostics must never take down the write path.
                log.warning("db.latency_flush_failed", error=str(exc))

        if written:
            self.stats.rows_written += written
            self.stats.batches += 1
            self.stats.last_flush_at = utcnow().isoformat()
        return written

    async def _periodic_flush(self) -> None:
        """Bound how long a row can sit unwritten on a quiet session."""
        while not self._stopping:
            try:
                await asyncio.sleep(self._settings.write_batch_timeout_s)
                await self.flush()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                log.error("db.periodic_flush_error", error=str(exc))
