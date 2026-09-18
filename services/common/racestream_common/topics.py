"""Kafka topic layout.

Grouping rationale (docs/architecture.md, "Topic and partition strategy"):

* Topics are split by *volume and consumer interest*, not by one-topic-per-type.
  Car telemetry and track location together are ~99% of the byte volume; a
  consumer that only needs lap times should not have to read them.
* Every topic is keyed ``session_id:driver_id``, so per-car ordering holds
  within a partition. Derivations that depend on order (sector deltas, stint
  state) are all per-car, so this is the ordering guarantee they need.
* Partition counts are sized so a session's ~20 drivers spread across them
  while leaving room to add consumer instances without a repartition.
"""

from __future__ import annotations

from dataclasses import dataclass

from racestream_common.schemas import EventType

TOPIC_PREFIX = "racestream"


@dataclass(frozen=True)
class TopicSpec:
    name: str
    partitions: int
    replication_factor: int
    retention_ms: int
    event_types: tuple[EventType, ...]
    note: str


_HOUR = 3_600_000
_DAY = 24 * _HOUR

TELEMETRY = TopicSpec(
    name=f"{TOPIC_PREFIX}.telemetry.v1",
    partitions=6,
    replication_factor=1,
    retention_ms=6 * _HOUR,
    event_types=(EventType.CAR_TELEMETRY,),
    note="Highest volume: ~3.7 Hz x 20 cars. Short retention; TimescaleDB is the archive.",
)

POSITION = TopicSpec(
    name=f"{TOPIC_PREFIX}.position.v1",
    partitions=6,
    replication_factor=1,
    retention_ms=6 * _HOUR,
    event_types=(EventType.POSITION,),
    note="Track x/y/z for the circuit map. Separate from telemetry so the map "
    "consumer can be scaled and restarted independently.",
)

TIMING = TopicSpec(
    name=f"{TOPIC_PREFIX}.timing.v1",
    partitions=3,
    replication_factor=1,
    retention_ms=_DAY,
    event_types=(EventType.LAP, EventType.TIMING),
    note="Lap and classification updates. Low volume, high value; kept a day.",
)

SESSION = TopicSpec(
    name=f"{TOPIC_PREFIX}.session.v1",
    partitions=1,
    replication_factor=1,
    retention_ms=7 * _DAY,
    event_types=(EventType.WEATHER, EventType.RACE_CONTROL, EventType.SESSION_META),
    note="Session-scoped events. One partition: global ordering matters more "
    "than throughput for race control.",
)

DEAD_LETTER = TopicSpec(
    name=f"{TOPIC_PREFIX}.dlq.v1",
    partitions=1,
    replication_factor=1,
    retention_ms=7 * _DAY,
    event_types=(),
    note="Messages that failed validation. Never blocks the main path.",
)

ALL_TOPICS: tuple[TopicSpec, ...] = (TELEMETRY, POSITION, TIMING, SESSION, DEAD_LETTER)

_ROUTE: dict[EventType, TopicSpec] = {
    et: spec for spec in ALL_TOPICS for et in spec.event_types
}


def topic_for(event_type: EventType) -> str:
    """Return the topic an event type is published to."""
    try:
        return _ROUTE[event_type].name
    except KeyError:  # pragma: no cover - guarded by the exhaustiveness test
        raise ValueError(f"no topic configured for event type {event_type!r}") from None


def spec_by_name(name: str) -> TopicSpec | None:
    return next((t for t in ALL_TOPICS if t.name == name), None)
