"""Stream processor.

Two consumer groups, split by what their work actually requires (see
``state.py`` for the full argument):

``racestream-telemetry``
    Reads ``telemetry.v1`` and ``position.v1`` - the high-volume per-car
    channels. Derives only per-car facts, so it is safe to run any number of
    instances. Scales horizontally up to the partition count.

``racestream-timing``
    Reads ``timing.v1`` and ``session.v1`` - low volume, but requires seeing
    every car to compute a session best. Run as a single instance. The topics
    are sized so that one instance is comfortably sufficient.

Both groups persist through the same batched, idempotent writer.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from datetime import datetime
from typing import Awaitable, Callable

from racestream_common.config import Settings
from racestream_common.db import Database, TelemetryWriter
from racestream_common.kafka import EventConsumer, EventProducer
from racestream_common.obs import METRICS, get_logger, observe_pipeline_latency
from racestream_common.obs.metrics import utcnow
from racestream_common.schemas import Event
from racestream_common.topics import POSITION, SESSION, TELEMETRY, TIMING

from racestream_processor.state import SessionState

log = get_logger(__name__)

TELEMETRY_GROUP = "racestream-telemetry"
TIMING_GROUP = "racestream-timing"

# Called with a list of processed events, after derivation and before the write
# is awaited. The WebSocket fanout attaches here in Phase 5.
StreamSink = Callable[[list[Event], dict], Awaitable[None]]


@dataclass
class ProcessorStats:
    events_processed: int = 0
    batches: int = 0
    sessions_seen: set[int] = field(default_factory=set)
    last_batch_at: str | None = None
    last_batch_size: int = 0
    last_error: str | None = None

    def as_dict(self) -> dict:
        return {
            "events_processed": self.events_processed,
            "batches": self.batches,
            "sessions_seen": sorted(self.sessions_seen),
            "last_batch_at": self.last_batch_at,
            "last_batch_size": self.last_batch_size,
            "last_error": self.last_error,
        }


class StreamProcessor:
    """Owns the consumers, the derived state and the database writer."""

    def __init__(
        self,
        settings: Settings,
        database: Database,
        sink: StreamSink | None = None,
        run_label: str | None = None,
    ) -> None:
        self._settings = settings
        self._database = database
        self._sink = sink

        self._writer = TelemetryWriter(database, settings.db, run_label=run_label)
        self._dlq_producer = EventProducer(settings.kafka, "processor-dlq")

        # One state object per session. A processor can legitimately see events
        # for several sessions at once - a replay running while a backfill is
        # still publishing, for instance.
        self._sessions: dict[int, SessionState] = {}

        self.stats = ProcessorStats()
        self.telemetry_consumer: EventConsumer | None = None
        self.timing_consumer: EventConsumer | None = None

    # ---------------------------------------------------------------- lifecycle

    async def start(self) -> None:
        await self._dlq_producer.start()
        await self._writer.start()

        self.telemetry_consumer = EventConsumer(
            settings=self._settings.kafka,
            topics=[TELEMETRY.name, POSITION.name],
            group_id=TELEMETRY_GROUP,
            handler=self._handle_batch,
            dlq_producer=self._dlq_producer,
        )
        self.timing_consumer = EventConsumer(
            settings=self._settings.kafka,
            topics=[TIMING.name, SESSION.name],
            group_id=TIMING_GROUP,
            handler=self._handle_batch,
            dlq_producer=self._dlq_producer,
        )

        await self.telemetry_consumer.start()
        await self.timing_consumer.start()
        METRICS.service_up.labels(component="processor").set(1)
        log.info("processor.started", groups=[TELEMETRY_GROUP, TIMING_GROUP])

    async def stop(self) -> None:
        # Consumers first: stop taking work before shutting down what handles it.
        for consumer in (self.telemetry_consumer, self.timing_consumer):
            if consumer is not None:
                await consumer.stop()
        # Flushes whatever is buffered, so the tail of a session is not lost.
        await self._writer.stop()
        await self._dlq_producer.stop()
        METRICS.service_up.labels(component="processor").set(0)
        log.info("processor.stopped", events=self.stats.events_processed)

    # ------------------------------------------------------------- processing

    async def _handle_batch(self, events: list[Event]) -> None:
        """Derive, stamp and persist one batch.

        Ordering within the batch is preserved: events arrive in partition order
        and the derivations depend on it.
        """
        if not events:
            return

        processed: list[Event] = []
        loop = asyncio.get_running_loop()
        batch_started = loop.time()

        for event in events:
            try:
                state = self._session_state(event.session_id)
                state.apply(event)

                stamped = event.model_copy(
                    update={"trace": event.trace.stamp("process_ts")}
                )
                processed.append(stamped)

                METRICS.messages_processed.labels(
                    event_type=stamped.event_type.value
                ).inc()
                observe_pipeline_latency(stamped.trace, stamped.event_type.value)
            except Exception as exc:
                # A derivation failure on one event must not lose the batch.
                self.stats.last_error = f"{type(exc).__name__}: {exc}"
                METRICS.errors.labels(
                    component="processor", kind=type(exc).__name__
                ).inc()
                METRICS.messages_dropped.labels(
                    reason="derivation_failed", stage="process"
                ).inc()
                log.warning(
                    "processor.derivation_failed",
                    event_type=event.event_type.value,
                    session_id=event.session_id,
                    driver_id=event.driver_id,
                    error=str(exc),
                )

        if not processed:
            return

        elapsed = loop.time() - batch_started
        METRICS.processing_latency.labels(
            event_type=processed[0].event_type.value
        ).observe(elapsed / len(processed))

        await self._writer.add(processed)

        # The sink is best-effort: a WebSocket client that has gone away must
        # never stall the pipeline that feeds the database.
        if self._sink is not None:
            try:
                await self._sink(processed, self.snapshot(processed[0].session_id))
            except Exception as exc:
                METRICS.errors.labels(component="processor.sink", kind=type(exc).__name__).inc()
                log.warning("processor.sink_failed", error=str(exc))

        self.stats.events_processed += len(processed)
        self.stats.batches += 1
        self.stats.last_batch_size = len(processed)
        self.stats.last_batch_at = utcnow().isoformat()

    def _session_state(self, session_id: int) -> SessionState:
        state = self._sessions.get(session_id)
        if state is None:
            state = SessionState(session_id=session_id)
            self._sessions[session_id] = state
            self.stats.sessions_seen.add(session_id)
            log.info("processor.session_started", session_id=session_id)
        return state

    # ------------------------------------------------------------------ views

    def snapshot(self, session_id: int) -> dict:
        """Current derived state for one session, for the API and WebSocket."""
        state = self._sessions.get(session_id)
        return state.as_dict() if state is not None else {"session_id": session_id, "drivers": []}

    def active_sessions(self) -> list[int]:
        return sorted(self._sessions)

    def health(self) -> dict:
        """What the System Health screen reports about this service."""
        return {
            "processor": self.stats.as_dict(),
            "writer": self._writer.stats.as_dict(),
            "consumers": [
                c.stats.as_dict()
                for c in (self.telemetry_consumer, self.timing_consumer)
                if c is not None
            ],
        }

    # ------------------------------------------------------------------ chaos

    def consumer(self, group_id: str) -> EventConsumer | None:
        """Look up a consumer by group, for the failure-injection endpoints."""
        mapping = {
            TELEMETRY_GROUP: self.telemetry_consumer,
            TIMING_GROUP: self.timing_consumer,
        }
        return mapping.get(group_id)
