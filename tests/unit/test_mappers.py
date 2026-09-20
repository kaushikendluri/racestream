"""Ingestion mapper tests.

The fixtures below are **verbatim records from the live OpenF1 API** (session
9472, 2024 Bahrain Grand Prix), captured with the probe in Phase 2. Using real
shapes rather than invented ones is the point: a mapper that passes against a
hand-written fixture proves only that it agrees with the author's assumptions.
"""

from __future__ import annotations

from datetime import datetime, timezone

import pytest

from racestream_ingestion.mappers import (
    MappingError,
    _parse_gap,
    map_car_telemetry,
    map_interval,
    map_lap,
    map_location,
    map_many,
    map_position,
    map_race_control,
    map_session_meta,
    map_weather,
    parse_timestamp,
)
from racestream_common.schemas import EventSource, EventType

# --------------------------------------------------------------- fixtures
# Captured from https://api.openf1.org/v1, session_key=9472.

CAR_DATA = {
    "date": "2024-03-02T15:10:00.070000+00:00",
    "session_key": 9472,
    "throttle": 0,
    "brake": 100,
    "rpm": 9238,
    "speed": 153,
    "driver_number": 1,
    "meeting_key": 1229,
    "n_gear": 4,
    "drs": 0,
}

LOCATION = {
    "date": "2024-03-02T15:10:00.139000+00:00",
    "session_key": 9472,
    "y": -3473,
    "z": -110,
    "meeting_key": 1229,
    "driver_number": 1,
    "x": 218,
}

LAP = {
    "meeting_key": 1229,
    "session_key": 9472,
    "driver_number": 1,
    "lap_number": 1,
    "date_start": "2024-03-02T15:03:42.341000+00:00",
    "duration_sector_1": 32.877,
    "duration_sector_2": 41.266,
    "duration_sector_3": 23.616,
    "lap_duration": 97.759,
    "i1_speed": 234,
    "i2_speed": 250,
    "st_speed": 251,
    "is_pit_out_lap": False,
    "segments_sector_1": [2049, 2049],
    "segments_sector_2": [2049],
    "segments_sector_3": [2049],
}

WEATHER = {
    "date": "2024-03-02T14:03:56.523000+00:00",
    "session_key": 9472,
    "air_temperature": 18.9,
    "pressure": 1017.1,
    "humidity": 46.0,
    "wind_direction": 162,
    "meeting_key": 1229,
    "rainfall": 0,
    "wind_speed": 0.9,
    "track_temperature": 26.5,
}

RACE_CONTROL = {
    "meeting_key": 1229,
    "session_key": 9472,
    # Note: no timezone offset. Other endpoints include one. Both must work.
    "date": "2024-03-02T14:06:41+00:00",
    "driver_number": None,
    "lap_number": 1,
    "category": "Other",
    "flag": None,
    "scope": None,
    "sector": None,
    "qualifying_phase": None,
    "message": "PINK HEAD PADDING MATERIAL MUST BE USED",
}

POSITION = {
    "date": "2024-03-02T14:03:47.739000+00:00",
    "session_key": 9472,
    "driver_number": 1,
    "position": 1,
    "meeting_key": 1229,
}

INTERVAL = {
    "date": "2024-03-02T14:03:46.275000+00:00",
    "session_key": 9472,
    "gap_to_leader": 0.0,
    "meeting_key": 1229,
    "driver_number": 1,
    "interval": 0.0,
}

SESSION = {
    "session_key": 9472,
    "meeting_key": 1229,
    "year": 2024,
    "session_name": "Race",
    "session_type": "Race",
    "circuit_short_name": "Sakhir",
    "country_name": "Bahrain",
    "location": "Sakhir",
    "date_start": "2024-03-02T15:00:00+00:00",
    "date_end": "2024-03-02T17:00:00+00:00",
    "gmt_offset": "03:00:00",
    "is_cancelled": False,
}


class TestTimestampParsing:
    def test_parses_an_offset_aware_timestamp(self):
        result = parse_timestamp("2024-03-02T15:10:00.070000+00:00")
        assert result.tzinfo == timezone.utc
        assert result.microsecond == 70000

    def test_treats_a_naive_timestamp_as_utc(self):
        """Some endpoints omit the offset; the API documents these as UTC."""
        assert parse_timestamp("2024-03-02T14:06:41") == datetime(
            2024, 3, 2, 14, 6, 41, tzinfo=timezone.utc
        )

    def test_normalises_a_non_utc_offset(self):
        assert parse_timestamp("2024-03-02T18:06:41+04:00") == datetime(
            2024, 3, 2, 14, 6, 41, tzinfo=timezone.utc
        )

    @pytest.mark.parametrize("value", [None, "", "not-a-date", 12345, []])
    def test_rejects_anything_unparseable(self, value):
        with pytest.raises(MappingError):
            parse_timestamp(value)


class TestCarTelemetryMapper:
    def test_maps_a_real_record(self):
        event = map_car_telemetry(CAR_DATA, EventSource.BACKFILL)
        assert event.event_type is EventType.CAR_TELEMETRY
        assert event.session_id == 9472
        assert event.driver_id == 1
        assert event.payload.speed == 153
        assert event.payload.brake == 100
        assert event.payload.n_gear == 4

    def test_stamps_source_and_ingest_times_only(self):
        """Ingestion can only honestly stamp the two stages it has performed."""
        trace = map_car_telemetry(CAR_DATA).trace
        assert trace.source_ts is not None
        assert trace.ingest_ts is not None
        assert trace.produce_ts is None
        assert trace.consume_ts is None
        assert trace.db_ts is None

    def test_rejects_an_out_of_range_reading_rather_than_clamping_it(self):
        """Clamping would turn a sensor glitch into a plausible-looking number."""
        event = map_car_telemetry({**CAR_DATA, "speed": 9999})
        assert event.payload.speed is None
        assert event.payload.rpm == 9238  # the rest of the sample survives

    def test_preserves_an_absent_channel_as_none(self):
        event = map_car_telemetry({**CAR_DATA, "drs": None})
        assert event.payload.drs is None
        assert event.payload.drs_open is None

    def test_distinguishes_a_measured_zero_from_an_absent_value(self):
        zeroed = map_car_telemetry({**CAR_DATA, "speed": 0})
        absent = map_car_telemetry({**CAR_DATA, "speed": None})
        assert zeroed.payload.speed == 0
        assert absent.payload.speed is None

    @pytest.mark.parametrize("field", ["session_key", "driver_number"])
    def test_rejects_a_record_missing_a_key_field(self, field):
        with pytest.raises(MappingError):
            map_car_telemetry({**CAR_DATA, field: None})


class TestLocationMapper:
    def test_maps_a_real_record(self):
        event = map_location(LOCATION)
        assert event.event_type is EventType.POSITION
        assert (event.payload.x, event.payload.y, event.payload.z) == (218.0, -3473.0, -110.0)

    def test_rejects_the_null_island_placeholder(self):
        """Upstream emits (0, 0) for a car not on track; plotting it would
        park a phantom car at the circuit origin."""
        with pytest.raises(MappingError, match="null island"):
            map_location({**LOCATION, "x": 0, "y": 0})

    def test_accepts_a_genuine_zero_on_one_axis(self):
        assert map_location({**LOCATION, "x": 0}).payload.x == 0.0

    def test_rejects_a_record_without_usable_coordinates(self):
        with pytest.raises(MappingError, match="no usable x/y"):
            map_location({**LOCATION, "y": None})

    def test_keeps_z_optional(self):
        assert map_location({**LOCATION, "z": None}).payload.z is None


class TestLapMapper:
    def test_maps_a_real_record(self):
        event = map_lap(LAP)
        assert event.payload.lap_number == 1
        assert event.payload.lap_duration == 97.759
        assert event.payload.duration_sector_2 == 41.266
        assert event.payload.st_speed == 251

    def test_uses_date_start_as_the_event_timestamp(self):
        """date_start is part of the primary key, so it must be authoritative."""
        assert map_lap(LAP).timestamp == parse_timestamp(LAP["date_start"])

    def test_ignores_upstream_fields_it_does_not_model(self):
        """segments_sector_* are present upstream but unused; they must not leak."""
        assert not hasattr(map_lap(LAP).payload, "segments_sector_1")

    def test_rejects_a_lap_with_no_start_time(self):
        with pytest.raises(MappingError):
            map_lap({**LAP, "date_start": None})

    def test_treats_a_non_positive_duration_as_absent(self):
        event = map_lap({**LAP, "lap_duration": 0, "duration_sector_1": -1.0})
        assert event.payload.lap_duration is None
        assert event.payload.duration_sector_1 is None

    def test_keeps_an_in_progress_lap_with_partial_sectors(self):
        """Upstream publishes a lap when it starts and fills sectors in later."""
        event = map_lap({**LAP, "lap_duration": None, "duration_sector_2": None,
                         "duration_sector_3": None})
        assert event.payload.duration_sector_1 == 32.877
        assert event.payload.lap_duration is None


class TestWeatherMapper:
    def test_maps_a_real_record(self):
        event = map_weather(WEATHER)
        assert event.driver_id is None  # weather is session-scoped
        assert event.payload.track_temperature == 26.5
        assert event.payload.rainfall == 0

    def test_discards_a_humidity_outside_physical_bounds(self):
        assert map_weather({**WEATHER, "humidity": 150}).payload.humidity is None


class TestRaceControlMapper:
    def test_maps_a_real_record(self):
        event = map_race_control(RACE_CONTROL)
        assert event.payload.message == "PINK HEAD PADDING MATERIAL MUST BE USED"
        assert event.payload.category == "Other"
        assert event.driver_id is None

    def test_keeps_a_driver_number_when_the_message_names_one(self):
        assert map_race_control({**RACE_CONTROL, "driver_number": 44}).driver_id == 44

    def test_rejects_a_message_free_record(self):
        with pytest.raises(MappingError, match="no message"):
            map_race_control({**RACE_CONTROL, "message": None})

    def test_truncates_an_overlong_message_rather_than_failing(self):
        event = map_race_control({**RACE_CONTROL, "message": "X" * 5000})
        assert len(event.payload.message) == 1024


class TestTimingMappers:
    def test_maps_a_classification_record(self):
        event = map_position(POSITION)
        assert event.event_type is EventType.TIMING
        assert event.payload.position == 1

    def test_maps_an_interval_record(self):
        event = map_interval(INTERVAL)
        assert event.payload.gap_to_leader == 0.0
        assert event.payload.interval == 0.0
        assert event.payload.laps_down is None

    @pytest.mark.parametrize(
        "raw,seconds,laps",
        [
            (0.0, 0.0, None),
            (2.104, 2.104, None),
            ("+1 LAP", None, 1),
            ("+2 LAPS", None, 2),
            ("+12.345", 12.345, None),
            (None, None, None),
        ],
    )
    def test_parses_both_gap_representations(self, raw, seconds, laps):
        """Upstream reports a gap either as seconds or as '+N LAP'. A lapped car
        has no meaningful seconds gap, so it must not be coerced into one."""
        assert _parse_gap(raw) == (seconds, laps)

    def test_a_lapped_car_reports_laps_down_and_no_seconds(self):
        event = map_interval({**INTERVAL, "gap_to_leader": "+1 LAP"})
        assert event.payload.laps_down == 1
        assert event.payload.gap_to_leader is None


class TestSessionMetaMapper:
    def test_maps_a_real_record(self):
        event = map_session_meta(SESSION, total_laps=57)
        assert event.payload.session_name == "Race"
        assert event.payload.circuit_short_name == "Sakhir"
        assert event.payload.total_laps == 57

    def test_total_laps_stays_none_when_not_yet_known(self):
        """It is derived from the lap data, which may not have been fetched."""
        assert map_session_meta(SESSION).payload.total_laps is None


class TestBulkMapping:
    def test_one_bad_record_does_not_cost_the_rest_of_the_batch(self):
        records = [CAR_DATA, {**CAR_DATA, "date": None}, CAR_DATA, {"garbage": True}]
        events, dropped = map_many("car_data", records)
        assert len(events) == 2
        assert dropped == 2

    def test_reports_zero_drops_for_a_clean_batch(self):
        events, dropped = map_many("car_data", [CAR_DATA] * 50)
        assert (len(events), dropped) == (50, 0)

    def test_propagates_the_source_to_every_event(self):
        events, _ = map_many("car_data", [CAR_DATA] * 3, EventSource.REPLAY)
        assert all(e.source is EventSource.REPLAY for e in events)

    def test_handles_an_empty_batch(self):
        assert map_many("weather", []) == ([], 0)

    def test_rejects_an_unregistered_endpoint(self):
        with pytest.raises(ValueError, match="no mapper registered"):
            map_many("not_an_endpoint", [])

    @pytest.mark.parametrize(
        "endpoint,record",
        [
            ("car_data", CAR_DATA),
            ("location", LOCATION),
            ("laps", LAP),
            ("weather", WEATHER),
            ("race_control", RACE_CONTROL),
            ("position", POSITION),
            ("intervals", INTERVAL),
        ],
    )
    def test_every_endpoint_maps_its_real_record(self, endpoint, record):
        events, dropped = map_many(endpoint, [record])
        assert (len(events), dropped) == (1, 0)
