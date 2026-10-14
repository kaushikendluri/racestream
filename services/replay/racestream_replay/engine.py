"""Replay engine.

The one rule this service exists to honour
-----------------------------------------
**Replay publishes the same envelopes to the same topics and is read by the same
consumer groups as live ingestion.** The only difference is ``source="replay"``,
which exists so the UI can be honest about what it is showing.

That is what makes replay more than a demo feature. Because the downstream path
cannot special-case replayed data - it has no way to tell - a replay exercises
the entire production pipeline deterministically. A replay test is therefore a
real integration test of a real-time system, which is otherwise very hard to
write.

The replay clock
----------------
Events carry their original timestamps. The engine reads them in time order and
sleeps the *scheduled gap* between consecutive events, divided by the speed
multiplier, so a 270ms gap at 1x is 270ms and at 10x is 27ms.

Pacing is against an absolute schedule rather than by sleeping each gap in turn.
Sleeping per-gap accumulates the overhead of every publish as drift, so a long
replay would fall progressively behind its own clock.
"""

from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import AsyncIterator

from racestream_common.config import Settings
from racestream_common.db import Database
from racestream_common.kafka import EventProducer
from racestream_common.obs import METRICS, get_logger
from racestream_common.obs.metrics import utcnow
from racestream_common.schemas import (
    CarTelemetry,
    Event,
    EventSource,
    EventType,
    LapCompleted,
    PipelineTrace,
    Position,
    RaceControl,
    SessionMeta,
    TimingUpdate,
    Weather,
)

log = get_logger(__name__)

VALID_SPEEDS = (0.5, 1.0, 2.0, 5.0, 10.0, 25.0)

# Rows are streamed from the database in chunks rather than loaded whole: a race
# is over a million rows and holding them all would defeat the point of having a
# time-series database.
FETCH_CHUNK = 5_000


class ReplayState(str, Enum):
    IDLE = "idle"
    LOADING = "loading"
    PLAYING = "playing"
    PAUSED = "paused"
    FINISHED = "finished"
    STOPPED = "stopped"
    FAILED = "failed"


@dataclass
class ReplayStatus:
    """What the replay controls display. Every field is observed."""

    state: ReplayState = ReplayState.IDLE
    session_id: int | None = None
    speed: float = 1.0
    events_published: int = 0
    events_total: int | None = None
    current_time: datetime | None = None
    window_start: datetime | None = None
    window_end: datetime | None = None
    current_lap: int | None = None
    total_laps: int | None = None
    started_at: datetime | None = None
    error: str | None = None

    @property
    def progress(self) -> float | None:
        """Fraction of the replay window emitted, 0..1.

        ``None`` until the window is known, rather than 0 - a progress bar that
        reads 0% before it knows the length is claiming something it cannot yet
        know.
        """
        if self.window_start is None or self.window_end is None or self.current_time is None:
            return None
        span = (self.window_end - self.window_start).total_seconds()
        if span <= 0:
            return None
        elapsed = (self.current_time - self.window_start).total_seconds()
        return max(0.0, min(1.0, elapsed / span))

    def as_dict(self) -> dict:
        return {
            "state": self.state.value,
            "session_id": self.session_id,
            "speed": self.speed,
            "events_published": self.events_published,
            "events_total": self.events_total,
            "progress": self.progress,
            "current_time": self.current_time.isoformat() if self.current_time else None,
            "window_start": self.window_start.isoformat() if self.window_start else None,
            "window_end": self.window_end.isoformat() if self.window_end else None,
            "current_lap": self.current_lap,
            "total_laps": self.total_laps,
            "started_at": self.started_at.isoformat() if self.started_at else None,
            "error": self.error,
        }


# --------------------------------------------------------------------- SQL
#
# One query per channel, each ordered by time. They are merged by the engine
# rather than by a UNION, so each row keeps its own column types and no casting
# is needed to force them into a common shape.

_QUERIES: dict[EventType, tuple[str, str]] = {
    EventType.CAR_TELEMETRY: (
        "sample_time",
        """
        SELECT driver_number, sample_time, speed, throttle, brake, n_gear, rpm, drs
        FROM car_telemetry
        WHERE session_id = $1 AND sample_time >= $2 AND sample_time < $3
        ORDER BY sample_time
        """,
    ),
    EventType.POSITION: (
        "sample_time",
        """
        SELECT driver_number, sample_time, x, y, z
        FROM positions
        WHERE session_id = $1 AND sample_time >= $2 AND sample_time < $3
        ORDER BY sample_time
        """,
    ),
    EventType.TIMING: (
        "sample_time",
        """
        SELECT driver_number, sample_time, position, gap_to_leader,
               interval_ahead, laps_down
        FROM timing
        WHERE session_id = $1 AND sample_time >= $2 AND sample_time < $3
        ORDER BY sample_time
        """,
    ),
    EventType.LAP: (
        "lap_start",
        """
        SELECT driver_number, lap_start, lap_number, lap_duration,
               duration_sector_1, duration_sector_2, duration_sector_3,
               i1_speed, i2_speed, st_speed, is_pit_out_lap
        FROM laps
        WHERE session_id = $1 AND lap_start >= $2 AND lap_start < $3
        ORDER BY lap_start
        """,
    ),
    EventType.WEATHER: (
        "sample_time",
        """
        SELECT sample_time, air_temperature, track_temperature, humidity,
               pressure, rainfall, wind_speed, wind_direction
        FROM weather
        WHERE session_id = $1 AND sample_time >= $2 AND sample_time < $3
        ORDER BY sample_time
        """,
    ),
    EventType.RACE_CONTROL: (
        "event_time",
        """
        SELECT event_time, driver_number, category, flag, scope, sector,
               lap_number, message
        FROM race_control_events
        WHERE session_id = $1 AND event_time >= $2 AND event_time < $3
        ORDER BY event_time
        """,
    ),
}


def _build_event(event_type: EventType, session_id: int, row: dict) -> Event | None:
    """Turn a stored row back into the event that produced it."""
    time_column = _QUERIES[event_type][0]
    timestamp = row[time_column]
    driver = row.get("driver_number")

    if event_type is EventType.CAR_TELEMETRY:
        payload = CarTelemetry(
            speed=row["speed"], throttle=row["throttle"], brake=row["brake"],
            n_gear=row["n_gear"], rpm=row["rpm"], drs=row["drs"],
        )
    elif event_type is EventType.POSITION:
        payload = Position(x=row["x"], y=row["y"], z=row["z"])
    elif event_type is EventType.TIMING:
        payload = TimingUpdate(
            position=row["position"], gap_to_leader=row["gap_to_leader"],
            interval=row["interval_ahead"], laps_down=row["laps_down"],
        )
    elif event_type is EventType.LAP:
        payload = LapCompleted(
            lap_number=row["lap_number"], lap_duration=row["lap_duration"],
            duration_sector_1=row["duration_sector_1"],
            duration_sector_2=row["duration_sector_2"],
            duration_sector_3=row["duration_sector_3"],
            i1_speed=row["i1_speed"], i2_speed=row["i2_speed"],
            st_speed=row["st_speed"], is_pit_out_lap=row["is_pit_out_lap"],
            date_start=row["lap_start"],
        )
    elif event_type is EventType.WEATHER:
        payload = Weather(
            air_temperature=row["air_temperature"],
            track_temperature=row["track_temperature"], humidity=row["humidity"],
            pressure=row["pressure"], rainfall=row["rainfall"],
            wind_speed=row["wind_speed"], wind_direction=row["wind_direction"],
        )
    elif event_type is EventType.RACE_CONTROL:
        payload = RaceControl(
            category=row["category"], flag=row["flag"], scope=row["scope"],
            sector=row["sector"], lap_number=row["lap_number"], message=row["message"],
        )
    else:
        return None

    return Event(
        event_type=event_type,
        # The honest part: real telemetry, correctly labelled as replayed.
        source=EventSource.REPLAY,
        session_id=session_id,
        driver_id=driver,
        timestamp=timestamp,
        payload=payload,
        # source_ts keeps the original recording time; ingest_ts is now, because
        # now is when this event entered the pipeline. Measuring latency against
        # a 2024 timestamp would report a latency of two years.
        trace=PipelineTrace(source_ts=timestamp).stamp("ingest_ts", utcnow()),
    )


class ReplayEngine:
    """Reads a stored session and re-emits it on a replay clock."""

    def __init__(self, settings: Settings, database: Database) -> None:
        self._settings = settings
        self._db = database
        self._producer: EventProducer | None = None

        self.status = ReplayStatus()
        self._task: asyncio.Task | None = None
        self._pause = asyncio.Event()
        self._pause.set()  # set == running
        self._stop = asyncio.Event()
        self._seek_to: datetime | None = None
        self._speed_changed = asyncio.Event()

    # ---------------------------------------------------------------- control

    async def start(
        self,
        session_id: int,
        speed: float = 1.0,
        start_at: datetime | None = None,
        from_lap: int | None = None,
    ) -> ReplayStatus:
        if self.status.state in (ReplayState.PLAYING, ReplayState.LOADING):
            raise RuntimeError(
                f"A replay of session {self.status.session_id} is already running. Stop it first."
            )
        if speed not in VALID_SPEEDS:
            raise ValueError(f"speed must be one of {VALID_SPEEDS}")

        window = await self._session_window(session_id)
        if window is None:
            raise LookupError(
                f"Session {session_id} has no replayable data. Ingest it first."
            )
        window_start, window_end, total_laps = window

        if from_lap is not None:
            lap_time = await self._time_of_lap(session_id, from_lap)
            if lap_time is not None:
                start_at = lap_time

        self.status = ReplayStatus(
            state=ReplayState.LOADING,
            session_id=session_id,
            speed=speed,
            window_start=window_start,
            window_end=window_end,
            current_time=start_at or window_start,
            total_laps=total_laps,
            started_at=utcnow(),
        )

        self._stop.clear()
        self._pause.set()
        self._task = asyncio.create_task(
            self._run(session_id, start_at or window_start, window_end),
            name=f"replay-{session_id}",
        )
        log.info(
            "replay.started",
            session_id=session_id,
            speed=speed,
            window_start=window_start.isoformat(),
            window_end=window_end.isoformat(),
        )
        return self.status

    def pause(self) -> ReplayStatus:
        if self.status.state is ReplayState.PLAYING:
            self._pause.clear()
            self.status.state = ReplayState.PAUSED
            log.info("replay.paused", session_id=self.status.session_id)
        return self.status

    def resume(self) -> ReplayStatus:
        if self.status.state is ReplayState.PAUSED:
            self._pause.set()
            self.status.state = ReplayState.PLAYING
            log.info("replay.resumed", session_id=self.status.session_id)
        return self.status

    def set_speed(self, speed: float) -> ReplayStatus:
        if speed not in VALID_SPEEDS:
            raise ValueError(f"speed must be one of {VALID_SPEEDS}")
        self.status.speed = speed
        # Wakes the pacing sleep so the new speed applies to the current gap
        # rather than only to the next one.
        self._speed_changed.set()
        log.info("replay.speed_changed", speed=speed)
        return self.status

    def seek(self, to: datetime) -> ReplayStatus:
        """Jump the replay clock. Takes effect at the next chunk boundary."""
        self._seek_to = to
        log.info("replay.seek", to=to.isoformat())
        return self.status

    async def stop(self) -> ReplayStatus:
        self._stop.set()
        self._pause.set()
        if self._task is not None:
            try:
                await asyncio.wait_for(self._task, timeout=10)
            except (asyncio.TimeoutError, asyncio.CancelledError):
                self._task.cancel()
            self._task = None
        if self.status.state not in (ReplayState.FINISHED, ReplayState.FAILED):
            self.status.state = ReplayState.STOPPED
        log.info(
            "replay.stopped",
            session_id=self.status.session_id,
            published=self.status.events_published,
        )
        return self.status

    # ----------------------------------------------------------------- queries

    async def _session_window(
        self, session_id: int
    ) -> tuple[datetime, datetime, int | None] | None:
        """The span to replay, taken from the data rather than the schedule.

        A session's nominal start and end do not match when telemetry actually
        exists, so the window comes from the recorded rows.
        """
        row = await self._db.fetchrow(
            """
            SELECT
              LEAST(
                (SELECT min(sample_time) FROM car_telemetry WHERE session_id = $1),
                (SELECT min(sample_time) FROM positions     WHERE session_id = $1),
                (SELECT min(sample_time) FROM timing        WHERE session_id = $1)
              ) AS first_event,
              GREATEST(
                (SELECT max(sample_time) FROM car_telemetry WHERE session_id = $1),
                (SELECT max(sample_time) FROM positions     WHERE session_id = $1),
                (SELECT max(sample_time) FROM timing        WHERE session_id = $1)
              ) AS last_event,
              (SELECT total_laps FROM sessions WHERE session_id = $1) AS total_laps
            """,
            session_id,
        )
        if row is None or row["first_event"] is None or row["last_event"] is None:
            return None
        return row["first_event"], row["last_event"], row["total_laps"]

    async def _time_of_lap(self, session_id: int, lap_number: int) -> datetime | None:
        return await self._db.fetchval(
            "SELECT min(lap_start) FROM laps WHERE session_id = $1 AND lap_number = $2",
            session_id,
            lap_number,
        )

    async def _load_chunk(
        self, session_id: int, start: datetime, end: datetime
    ) -> list[Event]:
        """Fetch one time window across every channel and merge it in time order."""
        events: list[Event] = []
        for event_type, (_, query) in _QUERIES.items():
            rows = await self._db.fetch(query, session_id, start, end)
            for row in rows:
                event = _build_event(event_type, session_id, dict(row))
                if event is not None:
                    events.append(event)

        # The merge: a single ordering across all channels, so the replay
        # reproduces the interleaving of the original session rather than
        # replaying each channel in turn.
        events.sort(key=lambda e: e.timestamp)
        return events

    # ---------------------------------------------------------------- main loop

    async def _run(self, session_id: int, start: datetime, end: datetime) -> None:
        from datetime import timedelta

        self._producer = EventProducer(self._settings.kafka, "replay")
        await self._producer.start()

        try:
            self.status.state = ReplayState.PLAYING
            cursor = start
            # Chunked by wall-clock span rather than row count, so the lookahead
            # is bounded in replay-time regardless of how dense the data is.
            chunk_span = timedelta(seconds=30)

            while cursor < end and not self._stop.is_set():
                if self._seek_to is not None:
                    cursor = self._seek_to
                    self._seek_to = None
                    self.status.current_time = cursor
                    log.info("replay.seeked", to=cursor.isoformat())
                    continue

                chunk_end = min(cursor + chunk_span, end)
                events = await self._load_chunk(session_id, cursor, chunk_end)

                if not events:
                    # A gap in the data. Honour it at the replay speed rather
                    # than skipping it, so a red-flag stoppage still feels like
                    # one at 1x.
                    await self._sleep_scaled(chunk_span.total_seconds())
                    cursor = chunk_end
                    self.status.current_time = cursor
                    continue

                await self._emit(events)
                cursor = chunk_end

            self.status.state = (
                ReplayState.STOPPED if self._stop.is_set() else ReplayState.FINISHED
            )
            log.info(
                "replay.complete",
                session_id=session_id,
                published=self.status.events_published,
                state=self.status.state.value,
            )

        except asyncio.CancelledError:
            self.status.state = ReplayState.STOPPED
            raise
        except Exception as exc:
            self.status.state = ReplayState.FAILED
            self.status.error = f"{type(exc).__name__}: {exc}"
            METRICS.errors.labels(component="replay", kind=type(exc).__name__).inc()
            log.error("replay.failed", session_id=session_id, error=str(exc))
        finally:
            if self._producer is not None:
                await self._producer.stop()
                self._producer = None

    async def _emit(self, events: list[Event]) -> None:
        """Publish a chunk, pacing against the original inter-event gaps."""
        assert self._producer is not None

        loop = asyncio.get_running_loop()
        schedule = loop.time()
        previous = events[0].timestamp

        for event in events:
            if self._stop.is_set():
                return
            # Blocks here while paused. The schedule is rebased on resume so a
            # pause does not cause a burst of catch-up publishing.
            if not self._pause.is_set():
                await self._pause.wait()
                schedule = loop.time()
                previous = event.timestamp

            gap = (event.timestamp - previous).total_seconds()
            if gap > 0:
                # Absolute schedule, not a per-gap sleep: sleeping each gap in
                # turn accumulates publish overhead as drift, so a long replay
                # would fall progressively behind its own clock.
                schedule += gap / self.status.speed
                delay = schedule - loop.time()
                if delay > 0:
                    await self._sleep_interruptible(delay)
            previous = event.timestamp

            # Re-stamp at publish time: the event is entering the pipeline now.
            stamped = event.model_copy(
                update={"trace": event.trace.model_copy(update={"ingest_ts": utcnow()})}
            )
            await self._producer.send(stamped)

            self.status.events_published += 1
            self.status.current_time = event.timestamp
            if event.event_type is EventType.LAP:
                self.status.current_lap = max(
                    self.status.current_lap or 0, event.payload.lap_number
                )

            METRICS.replay_events.labels(event_type=event.event_type.value).inc()

        progress = self.status.progress
        if progress is not None:
            METRICS.replay_progress.set(progress)

    async def _sleep_scaled(self, seconds: float) -> None:
        await self._sleep_interruptible(seconds / self.status.speed)

    async def _sleep_interruptible(self, seconds: float) -> None:
        """Sleep, but wake early on stop or a speed change.

        A plain sleep would mean a 10x speed change during a long gap did not
        take effect until that gap had elapsed at 1x.
        """
        self._speed_changed.clear()
        waiters = [
            asyncio.create_task(asyncio.sleep(seconds)),
            asyncio.create_task(self._stop.wait()),
            asyncio.create_task(self._speed_changed.wait()),
        ]
        done, pending = await asyncio.wait(waiters, return_when=asyncio.FIRST_COMPLETED)
        for task in pending:
            task.cancel()
