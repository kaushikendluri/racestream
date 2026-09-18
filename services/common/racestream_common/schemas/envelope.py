"""The RaceStream event envelope.

Design notes (see docs/engineering-decisions.md):

* One envelope for every topic keeps consumers, metrics and the replay engine
  uniform: a consumer can route on ``event_type`` without knowing the topic.
* ``schema_version`` is carried per-event rather than per-topic so a producer
  rollout can be gradual.
* ``trace`` accumulates timestamps as the event moves through the pipeline.
  Each stage stamps its own field, which is what makes honest end-to-end
  latency measurement possible (section 20 of the spec) rather than guessed.
"""

from __future__ import annotations

import uuid
from datetime import datetime, timezone
from enum import Enum
from typing import Annotated, Union

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from racestream_common.schemas.payloads import (
    CarTelemetry,
    LapCompleted,
    Position,
    RaceControl,
    SessionMeta,
    TimingUpdate,
    Weather,
)

SCHEMA_VERSION = 1
"""Current envelope schema version. Bump only on a breaking envelope change."""


class EventType(str, Enum):
    """Discriminator for the payload union."""

    CAR_TELEMETRY = "car_telemetry"
    POSITION = "position"
    TIMING = "timing"
    LAP = "lap"
    WEATHER = "weather"
    RACE_CONTROL = "race_control"
    SESSION_META = "session_meta"


class EventSource(str, Enum):
    """Where an event entered the system.

    The frontend uses this to render the LIVE / REPLAY indicator honestly.
    Downstream processing is deliberately identical for both.
    """

    LIVE = "live"
    REPLAY = "replay"
    BACKFILL = "backfill"


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


class PipelineTrace(BaseModel):
    """Timestamps stamped by each pipeline stage.

    All fields are optional: a stage that has not run yet leaves its field
    ``None``, and the UI renders ``N/A`` rather than inventing a number.
    """

    model_config = ConfigDict(extra="forbid")

    source_ts: datetime | None = Field(
        default=None,
        description="Timestamp reported by the upstream data source itself.",
    )
    ingest_ts: datetime | None = Field(
        default=None, description="When the ingestion service built this envelope."
    )
    produce_ts: datetime | None = Field(
        default=None, description="Just before the Kafka produce call returned."
    )
    consume_ts: datetime | None = Field(
        default=None, description="When the stream processor received the record."
    )
    process_ts: datetime | None = Field(
        default=None, description="When processing/derivation finished."
    )
    db_ts: datetime | None = Field(
        default=None, description="When the batch containing this event was committed."
    )
    ws_ts: datetime | None = Field(
        default=None, description="When the WebSocket fanout wrote the frame."
    )

    def stamp(self, field: str, when: datetime | None = None) -> "PipelineTrace":
        """Return a copy with ``field`` set. Events are treated as immutable."""
        if field not in self.model_fields:
            raise ValueError(f"unknown trace field: {field}")
        return self.model_copy(update={field: when or _utcnow()})

    def elapsed_seconds(self, start: str, end: str) -> float | None:
        """Seconds between two stages, or ``None`` if either is unstamped."""
        a, b = getattr(self, start), getattr(self, end)
        if a is None or b is None:
            return None
        return (b - a).total_seconds()


Payload = Annotated[
    Union[
        CarTelemetry,
        Position,
        TimingUpdate,
        LapCompleted,
        Weather,
        RaceControl,
        SessionMeta,
    ],
    Field(discriminator="kind"),
]


class Event(BaseModel):
    """The single envelope carried on every RaceStream topic."""

    model_config = ConfigDict(extra="forbid", use_enum_values=False)

    schema_version: int = Field(default=SCHEMA_VERSION, ge=1)
    event_id: str = Field(
        default_factory=lambda: str(uuid.uuid4()),
        description="Unique per logical event; the idempotency key for consumers.",
        max_length=128,
    )
    event_type: EventType
    source: EventSource
    session_id: int = Field(description="OpenF1 session_key.")
    driver_id: int | None = Field(
        default=None, description="Driver number; None for session-scoped events."
    )
    timestamp: datetime = Field(
        description="Authoritative event time (source time when available)."
    )
    trace: PipelineTrace = Field(default_factory=PipelineTrace)
    payload: Payload

    @model_validator(mode="after")
    def _payload_matches_type(self) -> "Event":
        """The envelope tag and the payload tag must agree.

        They are redundant on purpose: ``event_type`` lets a consumer route
        without deserialising the payload. Redundancy is only safe if it is
        checked, so it is checked here, at the boundary.
        """
        if self.payload.kind != self.event_type.value:
            raise ValueError(
                f"event_type={self.event_type.value!r} does not match "
                f"payload.kind={self.payload.kind!r}"
            )
        return self

    @field_validator("timestamp")
    @classmethod
    def _require_tz(cls, v: datetime) -> datetime:
        """Reject naive datetimes outright: an ambiguous clock corrupts latency."""
        if v.tzinfo is None:
            raise ValueError("timestamp must be timezone-aware")
        return v.astimezone(timezone.utc)

    @property
    def partition_key(self) -> bytes:
        """Kafka partition key.

        Keyed by ``session:driver`` so that all events for one car land on one
        partition and therefore stay ordered relative to each other, which is
        what sector/lap derivation requires. Session-scoped events (weather,
        race control) key on the session alone. See docs/architecture.md.
        """
        driver = self.driver_id if self.driver_id is not None else "_"
        return f"{self.session_id}:{driver}".encode()

    @property
    def dedupe_key(self) -> str:
        """Natural key used for idempotent upserts.

        ``event_id`` is a UUID for live ingestion, so it is *not* stable across
        a replay of the same source rows. The natural key is, which is why the
        database uniqueness constraints use this instead. See docs/data-contracts.md.
        """
        driver = self.driver_id if self.driver_id is not None else "_"
        ts = self.timestamp.astimezone(timezone.utc).isoformat()
        et = self.event_type.value if isinstance(self.event_type, EventType) else self.event_type
        return f"{self.session_id}|{driver}|{ts}|{et}"
