"""Event contract tests.

These are the highest-value tests in the project. The envelope is the interface
between five services; if it silently accepts something malformed, the damage
shows up three hops away as a wrong number on a screen.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest
from pydantic import ValidationError

from racestream_common.kafka.serde import (
    DeserializationError,
    deserialize_event,
    serialize_event,
)
from racestream_common.schemas import (
    PAYLOAD_MODELS,
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
from racestream_common.topics import ALL_TOPICS, topic_for

UTC_NOW = datetime(2026, 7, 7, 14, 30, 0, tzinfo=timezone.utc)


def make_event(**overrides) -> Event:
    defaults = dict(
        event_type=EventType.CAR_TELEMETRY,
        source=EventSource.LIVE,
        session_id=9158,
        driver_id=1,
        timestamp=UTC_NOW,
        payload=CarTelemetry(speed=300, throttle=100, brake=0, n_gear=8, rpm=11000),
    )
    defaults.update(overrides)
    return Event(**defaults)


class TestEnvelope:
    def test_roundtrips_through_the_wire_format(self):
        event = make_event()
        restored = deserialize_event(serialize_event(event))
        assert restored.event_id == event.event_id
        assert restored.payload == event.payload
        assert restored.timestamp == event.timestamp

    def test_rejects_a_naive_timestamp(self):
        """An ambiguous clock silently corrupts every latency measurement."""
        with pytest.raises(ValidationError, match="timezone-aware"):
            make_event(timestamp=datetime(2026, 7, 7, 14, 30, 0))

    def test_normalises_a_non_utc_timestamp_to_utc(self):
        tokyo = timezone(timedelta(hours=9))
        event = make_event(timestamp=datetime(2026, 7, 7, 23, 30, 0, tzinfo=tokyo))
        assert event.timestamp == UTC_NOW
        assert event.timestamp.tzinfo == timezone.utc

    def test_rejects_a_payload_that_contradicts_the_event_type(self):
        """The two tags are redundant, so they must be checked against each other."""
        with pytest.raises(ValidationError, match="does not match"):
            make_event(event_type=EventType.POSITION)

    def test_rejects_unknown_envelope_fields(self):
        with pytest.raises(ValidationError):
            Event(
                event_type=EventType.POSITION,
                source=EventSource.LIVE,
                session_id=1,
                timestamp=UTC_NOW,
                payload=Position(x=1.0, y=2.0),
                unexpected_field="surprise",
            )

    def test_event_ids_are_unique_per_event(self):
        assert make_event().event_id != make_event().event_id


class TestPartitioning:
    def test_keys_by_session_and_driver_so_per_car_order_holds(self):
        assert make_event(session_id=9158, driver_id=16).partition_key == b"9158:16"

    def test_session_scoped_events_key_on_the_session_alone(self):
        event = make_event(
            event_type=EventType.WEATHER,
            driver_id=None,
            payload=Weather(air_temperature=24.0, track_temperature=41.2),
        )
        assert event.partition_key == b"9158:_"

    def test_same_car_always_lands_on_the_same_key(self):
        """Ordering guarantees depend on this being stable across event types."""
        a = make_event(timestamp=UTC_NOW)
        b = make_event(timestamp=UTC_NOW + timedelta(seconds=5))
        c = make_event(
            event_type=EventType.POSITION,
            payload=Position(x=1.0, y=2.0, z=0.0),
        )
        assert a.partition_key == b.partition_key == c.partition_key


class TestIdempotencyKey:
    def test_natural_key_is_stable_across_separate_ingestion_runs(self):
        """event_id is a fresh UUID each run, so it cannot be the dedupe key."""
        first, second = make_event(), make_event()
        assert first.event_id != second.event_id
        assert first.dedupe_key == second.dedupe_key

    def test_key_separates_drivers_times_and_types(self):
        base = make_event()
        assert make_event(driver_id=16).dedupe_key != base.dedupe_key
        assert make_event(timestamp=UTC_NOW + timedelta(milliseconds=1)).dedupe_key != base.dedupe_key
        assert (
            make_event(
                event_type=EventType.POSITION, payload=Position(x=0.0, y=0.0)
            ).dedupe_key
            != base.dedupe_key
        )


class TestPipelineTrace:
    def test_unstamped_stages_report_none_rather_than_zero(self):
        """This is what lets the UI show N/A instead of inventing a latency."""
        trace = PipelineTrace().stamp("ingest_ts")
        assert trace.elapsed_seconds("ingest_ts", "ws_ts") is None
        assert trace.ws_ts is None

    def test_measures_the_gap_between_two_stamped_stages(self):
        start = UTC_NOW
        trace = PipelineTrace().stamp("ingest_ts", start).stamp(
            "produce_ts", start + timedelta(milliseconds=12)
        )
        assert trace.elapsed_seconds("ingest_ts", "produce_ts") == pytest.approx(0.012)

    def test_stamping_does_not_mutate_the_original(self):
        original = PipelineTrace()
        stamped = original.stamp("ingest_ts")
        assert original.ingest_ts is None
        assert stamped.ingest_ts is not None

    def test_rejects_an_unknown_stage_name(self):
        with pytest.raises(ValueError, match="unknown trace field"):
            PipelineTrace().stamp("not_a_stage")

    def test_trace_survives_serialisation(self):
        event = make_event(trace=PipelineTrace().stamp("ingest_ts", UTC_NOW))
        restored = deserialize_event(serialize_event(event))
        assert restored.trace.ingest_ts == UTC_NOW


class TestPayloadValidation:
    @pytest.mark.parametrize(
        "field,value",
        [
            ("speed", 401),      # above any real F1 speed
            ("speed", -1),
            ("throttle", 101),
            ("n_gear", 9),       # an F1 car has eight forward gears
            ("rpm", 25000),
            ("drs", 15),
        ],
    )
    def test_rejects_physically_impossible_telemetry(self, field, value):
        with pytest.raises(ValidationError):
            CarTelemetry(**{field: value})

    def test_accepts_a_fully_absent_sample(self):
        """Upstream omits channels; absence is legal, nonsense is not."""
        assert CarTelemetry().speed is None

    @pytest.mark.parametrize(
        "code,expected",
        [(0, False), (1, False), (8, False), (10, True), (12, True), (14, True)],
    )
    def test_maps_documented_drs_codes(self, code, expected):
        assert CarTelemetry(drs=code).drs_open is expected

    @pytest.mark.parametrize("code", [2, 3, 9])
    def test_undocumented_drs_codes_resolve_to_unknown_not_a_guess(self, code):
        assert CarTelemetry(drs=code).drs_open is None

    def test_rejects_a_non_positive_lap_duration(self):
        with pytest.raises(ValidationError):
            LapCompleted(lap_number=1, lap_duration=0)

    def test_rejects_an_oversized_race_control_message(self):
        with pytest.raises(ValidationError):
            RaceControl(message="x" * 1025)

    def test_every_event_type_has_a_payload_model(self):
        assert set(PAYLOAD_MODELS) == {t.value for t in EventType}


class TestDeserialisation:
    @pytest.mark.parametrize(
        "raw",
        [
            b"",
            b"not json at all",
            b'{"broken": true}',
            b"[]",
            b'{"event_type": "car_telemetry"}',        # missing the rest
            b'{"event_type": "no_such_type", "source": "live", "session_id": 1,'
            b' "timestamp": "2026-07-07T14:30:00Z", "payload": {"kind": "car_telemetry"}}',
        ],
    )
    def test_rejects_malformed_messages_without_raising_anything_unexpected(self, raw):
        """Poison messages must fail as DeserializationError so the consumer can DLQ them."""
        with pytest.raises(DeserializationError) as exc:
            deserialize_event(raw)
        assert exc.value.raw == raw  # the original bytes reach the DLQ intact


class TestTopicRouting:
    @pytest.mark.parametrize("event_type", list(EventType))
    def test_every_event_type_routes_to_a_topic(self, event_type):
        assert topic_for(event_type)

    def test_high_volume_types_are_separated_from_low_volume_ones(self):
        """A consumer wanting lap times should not have to read telemetry."""
        assert topic_for(EventType.CAR_TELEMETRY) != topic_for(EventType.LAP)
        assert topic_for(EventType.POSITION) != topic_for(EventType.CAR_TELEMETRY)

    def test_session_scoped_types_share_one_ordered_partition(self):
        session_topics = {
            topic_for(t)
            for t in (EventType.WEATHER, EventType.RACE_CONTROL, EventType.SESSION_META)
        }
        assert len(session_topics) == 1

    def test_topic_names_are_versioned(self):
        """A breaking schema change gets a new topic, not a reinterpreted one."""
        assert all(t.name.endswith(".v1") for t in ALL_TOPICS)

    def test_partition_counts_leave_room_to_scale_consumers(self):
        telemetry = next(t for t in ALL_TOPICS if "telemetry" in t.name)
        assert telemetry.partitions > 1


class TestAllPayloadTypes:
    """Each payload type must survive a full round-trip on its own topic."""

    @pytest.mark.parametrize(
        "event_type,payload,driver",
        [
            (EventType.CAR_TELEMETRY, CarTelemetry(speed=312, rpm=11800), 1),
            (EventType.POSITION, Position(x=1234.5, y=-987.6, z=12.0), 1),
            (EventType.TIMING, TimingUpdate(position=1, gap_to_leader=0.0), 1),
            (EventType.LAP, LapCompleted(lap_number=38, lap_duration=91.204), 1),
            (EventType.WEATHER, Weather(air_temperature=24.1, rainfall=0), None),
            (EventType.RACE_CONTROL, RaceControl(message="SAFETY CAR DEPLOYED"), None),
            (
                EventType.SESSION_META,
                SessionMeta(session_name="Race", session_type="Race", year=2026),
                None,
            ),
        ],
    )
    def test_roundtrip(self, event_type, payload, driver):
        event = Event(
            event_type=event_type,
            source=EventSource.REPLAY,
            session_id=9158,
            driver_id=driver,
            timestamp=UTC_NOW,
            payload=payload,
        )
        restored = deserialize_event(serialize_event(event))
        assert restored.event_type is event_type
        assert restored.payload == payload
        assert topic_for(restored.event_type)
