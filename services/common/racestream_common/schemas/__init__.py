"""RaceStream data contracts.

Every message that crosses a service boundary is an :class:`Event` - a stable
envelope carrying a versioned, type-tagged payload. See docs/data-contracts.md.
"""

from racestream_common.schemas.envelope import (
    SCHEMA_VERSION,
    Event,
    EventSource,
    EventType,
    PipelineTrace,
)
from racestream_common.schemas.payloads import (
    CarTelemetry,
    LapCompleted,
    PAYLOAD_MODELS,
    Position,
    RaceControl,
    SessionMeta,
    TimingUpdate,
    Weather,
)

__all__ = [
    "SCHEMA_VERSION",
    "Event",
    "EventSource",
    "EventType",
    "PipelineTrace",
    "CarTelemetry",
    "LapCompleted",
    "Position",
    "RaceControl",
    "SessionMeta",
    "TimingUpdate",
    "Weather",
    "PAYLOAD_MODELS",
]
