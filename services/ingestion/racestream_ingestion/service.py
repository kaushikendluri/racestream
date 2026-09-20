"""Ingestion orchestration.

Fetch a session from OpenF1, map it to events, publish it to Kafka.

The service deliberately does **not** write telemetry to the database. Its only
outputs are Kafka messages and the metadata tier. That keeps a single writer for
the telemetry tables (the stream processor) and means the replay path and the
ingest path converge at exactly one point: the topic.

Windowing
---------
Car telemetry and track position are fetched in time windows rather than whole.
Measured: a 300s window across all drivers is ~22k records and ~3.7MB, returned
in about two seconds. A two-hour race is therefore a few dozen requests per
channel, which sits comfortably inside the upstream limit of 3 requests/second.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any

from racestream_common.config import Settings
from racestream_common.db import Database
from racestream_common.kafka import EventProducer
from racestream_common.obs import METRICS, get_logger
from racestream_common.schemas import Event, EventSource

from racestream_ingestion.mappers import map_many, map_session_meta
from racestream_ingestion.metadata import MetadataRepository
from racestream_ingestion.openf1 import OpenF1Client, OpenF1Error, chunk_windows

log = get_logger(__name__)

# Channels fetched whole, because they are small and not time-partitioned.
_BULK_CHANNELS = (
    ("laps", "laps"),
    ("weather", "weather"),
    ("race_control", "race_control"),
    ("position", "position"),
    ("intervals", "intervals"),
)


@dataclass
class IngestReport:
    """What actually happened. Returned to the CLI and written to the session row."""

    session_id: int
    status: str = "running"
    published: dict[str, int] = field(default_factory=dict)
    dropped: dict[str, int] = field(default_factory=dict)
    errors: list[str] = field(default_factory=list)
    drivers: int = 0
    stints: int = 0
    total_laps: int | None = None
    started_at: datetime | None = None
    finished_at: datetime | None = None

    @property
    def total_published(self) -> int:
        return sum(self.published.values())

    @property
    def total_dropped(self) -> int:
        return sum(self.dropped.values())

    @property
    def duration_seconds(self) -> float | None:
        if self.started_at is None or self.finished_at is None:
            return None
        return (self.finished_at - self.started_at).total_seconds()

    def channels(self) -> dict[str, bool]:
        """Map published counts to the availability flags on the session row."""
        return {
            "car_telemetry": self.published.get("car_data", 0) > 0,
            "position": self.published.get("location", 0) > 0,
            "laps": self.published.get("laps", 0) > 0,
            "weather": self.published.get("weather", 0) > 0,
            "race_control": self.published.get("race_control", 0) > 0,
        }

    def as_dict(self) -> dict[str, Any]:
        return {
            "session_id": self.session_id,
            "status": self.status,
            "published": dict(self.published),
            "dropped": dict(self.dropped),
            "total_published": self.total_published,
            "total_dropped": self.total_dropped,
            "drivers": self.drivers,
            "stints": self.stints,
            "total_laps": self.total_laps,
            "duration_seconds": self.duration_seconds,
            "errors": self.errors,
            "channels": self.channels(),
        }


class IngestionService:
    def __init__(
        self,
        settings: Settings,
        client: OpenF1Client,
        producer: EventProducer,
        database: Database,
        window_seconds: int = 300,
    ) -> None:
        self._settings = settings
        self._client = client
        self._producer = producer
        self._metadata = MetadataRepository(database)
        self._window_seconds = window_seconds

    async def ingest_session(
        self,
        session_id: int,
        source: EventSource = EventSource.BACKFILL,
        include_telemetry: bool = True,
        max_windows: int | None = None,
    ) -> IngestReport:
        """Ingest one session.

        ``max_windows`` bounds the telemetry fetch, which is what makes a quick
        smoke run possible without pulling a full race.
        """
        report = IngestReport(session_id=session_id, started_at=_now())
        log.info("ingest.started", session_id=session_id, source=source.value)

        # --- session identity -------------------------------------------
        try:
            records = await self._client.sessions(session_key=session_id)
        except OpenF1Error as exc:
            report.status = "failed"
            report.errors.append(f"sessions: {exc}")
            report.finished_at = _now()
            log.error("ingest.session_lookup_failed", session_id=session_id, error=str(exc))
            return report

        if not records:
            report.status = "failed"
            report.errors.append(f"upstream has no session with key {session_id}")
            report.finished_at = _now()
            log.error("ingest.session_not_found", session_id=session_id)
            return report

        session_record = records[0]
        await self._metadata.upsert_session(session_record, status="running")

        # --- reference data ---------------------------------------------
        for label, fetch, apply in (
            ("drivers", self._client.drivers, self._apply_drivers),
            ("stints", self._client.stints, self._apply_stints),
        ):
            try:
                await apply(session_id, await fetch(session_id), report)
            except OpenF1Error as exc:
                # Reference data is useful but not fatal: telemetry still has
                # meaning without a headshot URL.
                report.errors.append(f"{label}: {exc}")
                log.warning("ingest.reference_failed", channel=label, error=str(exc))

        # --- session_meta so the stream is self-describing ---------------
        try:
            meta_event = map_session_meta(session_record, report.total_laps, source)
            await self._producer.send(meta_event, wait=True)
            report.published["session_meta"] = 1
        except Exception as exc:
            report.errors.append(f"session_meta: {exc}")
            log.warning("ingest.session_meta_failed", error=str(exc))

        # --- bulk channels ----------------------------------------------
        for endpoint, label in _BULK_CHANNELS:
            await self._ingest_bulk(session_id, endpoint, label, source, report)

        # --- windowed telemetry -----------------------------------------
        if include_telemetry:
            await self._ingest_windowed(session_id, session_record, source, report, max_windows)

        await self._producer.flush()

        report.finished_at = _now()
        report.status = self._final_status(report)
        await self._metadata.record_availability(
            session_id, report.channels(), report.total_laps, report.status
        )

        log.info(
            "ingest.finished",
            session_id=session_id,
            status=report.status,
            published=report.total_published,
            dropped=report.total_dropped,
            duration_s=round(report.duration_seconds or 0, 1),
        )
        return report

    # ------------------------------------------------------------ internals

    async def _apply_drivers(
        self, session_id: int, records: list[dict], report: IngestReport
    ) -> None:
        report.drivers = await self._metadata.upsert_drivers(session_id, records)

    async def _apply_stints(
        self, session_id: int, records: list[dict], report: IngestReport
    ) -> None:
        report.stints = await self._metadata.upsert_stints(session_id, records)

    async def _ingest_bulk(
        self,
        session_id: int,
        endpoint: str,
        label: str,
        source: EventSource,
        report: IngestReport,
    ) -> None:
        """Fetch, map and publish a channel that is small enough to take whole."""
        try:
            if endpoint == "position":
                raw = await self._client.positions(session_id)
            elif endpoint == "intervals":
                raw = await self._client.intervals(session_id)
            elif endpoint == "laps":
                raw = await self._client.laps(session_id)
            elif endpoint == "weather":
                raw = await self._client.weather(session_id)
            else:
                raw = await self._client.race_control(session_id)
        except OpenF1Error as exc:
            # A channel that is unavailable is recorded as unavailable, not
            # faked and not fatal to the rest of the session.
            report.errors.append(f"{label}: {exc}")
            report.published.setdefault(label, 0)
            log.warning("ingest.channel_failed", channel=label, error=str(exc))
            return

        events, dropped = map_many(endpoint, raw, source)
        sent = await self._producer.send_many(events)

        report.published[label] = report.published.get(label, 0) + sent
        report.dropped[label] = report.dropped.get(label, 0) + dropped

        if endpoint == "laps" and raw:
            numbers = [r.get("lap_number") for r in raw if isinstance(r.get("lap_number"), int)]
            report.total_laps = max(numbers) if numbers else None

        log.info(
            "ingest.channel_complete",
            channel=label,
            fetched=len(raw),
            published=sent,
            dropped=dropped,
        )

    async def _ingest_windowed(
        self,
        session_id: int,
        session_record: dict,
        source: EventSource,
        report: IngestReport,
        max_windows: int | None,
    ) -> None:
        """Fetch car telemetry and track position window by window."""
        start, end = self._resolve_window(session_record)
        if start is None or end is None:
            report.errors.append(
                "telemetry: session has no start/end time, so no window could be built"
            )
            log.warning("ingest.no_time_window", session_id=session_id)
            return

        windows = chunk_windows(start, end, self._window_seconds)
        if max_windows is not None:
            windows = windows[:max_windows]

        log.info(
            "ingest.telemetry_windows",
            session_id=session_id,
            windows=len(windows),
            window_seconds=self._window_seconds,
            span_start=start.isoformat(),
            span_end=end.isoformat(),
        )

        for index, (w_start, w_end) in enumerate(windows, start=1):
            for endpoint, label, fetch in (
                ("car_data", "car_data", self._client.car_data),
                ("location", "location", self._client.location),
            ):
                try:
                    raw = await fetch(session_id, w_start, w_end)
                except OpenF1Error as exc:
                    # One bad window must not abandon the session; the gap is
                    # recorded so it is visible rather than silently missing.
                    report.errors.append(f"{label} window {index}: {exc}")
                    log.warning(
                        "ingest.window_failed",
                        channel=label,
                        window=index,
                        error=str(exc),
                    )
                    continue

                events, dropped = map_many(endpoint, raw, source)
                sent = await self._producer.send_many(events)
                report.published[label] = report.published.get(label, 0) + sent
                report.dropped[label] = report.dropped.get(label, 0) + dropped

            if index % 10 == 0 or index == len(windows):
                METRICS.queue_depth.labels(queue="ingest_windows").set(len(windows) - index)
                log.info(
                    "ingest.progress",
                    session_id=session_id,
                    window=index,
                    of=len(windows),
                    published=report.total_published,
                )

    def _resolve_window(
        self, session_record: dict
    ) -> tuple[datetime | None, datetime | None]:
        """Work out the time span to fetch telemetry over.

        Padded by a minute at each end: cars are on track before the session
        clock starts and after it stops, and clipping to the nominal window
        loses the out-lap and the in-lap.
        """
        from racestream_ingestion.mappers import parse_timestamp

        raw_start = session_record.get("date_start")
        raw_end = session_record.get("date_end")
        if not raw_start or not raw_end:
            return None, None
        try:
            start = parse_timestamp(raw_start) - timedelta(minutes=1)
            end = parse_timestamp(raw_end) + timedelta(minutes=1)
        except Exception:
            return None, None
        return start, end

    @staticmethod
    def _final_status(report: IngestReport) -> str:
        """Report honestly: 'partial' is a real outcome and is not hidden."""
        if report.total_published == 0:
            return "failed"
        if report.errors:
            return "partial"
        return "complete"


def _now() -> datetime:
    return datetime.now(timezone.utc)
