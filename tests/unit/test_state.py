"""Derived-state tests.

These cover the derivation rules that are easy to get subtly wrong and hard to
notice: partial sector arrival, a later null erasing an earlier value, lapped
cars, and personal versus session bests.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from racestream_common.schemas import (
    CarTelemetry,
    Event,
    EventSource,
    EventType,
    LapCompleted,
    Position,
    SessionMeta,
    TimingUpdate,
    Weather,
)
from racestream_processor.state import DriverState, SectorTimes, SessionState

T0 = datetime(2024, 3, 2, 15, 0, 0, tzinfo=timezone.utc)
SESSION = 9472


def ev(event_type, payload, driver=1, offset_s=0.0) -> Event:
    return Event(
        event_type=event_type,
        source=EventSource.REPLAY,
        session_id=SESSION,
        driver_id=driver,
        timestamp=T0 + timedelta(seconds=offset_s),
        payload=payload,
    )


def lap(number, duration=None, s1=None, s2=None, s3=None, driver=1, offset_s=0.0):
    return ev(
        EventType.LAP,
        LapCompleted(
            lap_number=number,
            lap_duration=duration,
            duration_sector_1=s1,
            duration_sector_2=s2,
            duration_sector_3=s3,
            date_start=T0 + timedelta(seconds=offset_s),
        ),
        driver,
        offset_s,
    )


class TestSectorTimes:
    def test_a_later_null_never_erases_a_known_sector(self):
        """Upstream republishes a lap as sectors complete, with the not-yet-set
        sectors null. Overwriting would lose the sector we already had."""
        have = SectorTimes(s1=29.4, s2=38.1)
        merged = have.merge(SectorTimes(s3=24.0))
        assert merged.as_tuple() == (29.4, 38.1, 24.0)

    def test_a_later_value_supersedes_an_earlier_one(self):
        assert SectorTimes(s1=30.0).merge(SectorTimes(s1=29.4)).s1 == 29.4

    def test_completeness_requires_all_three(self):
        assert not SectorTimes(s1=29.4, s2=38.1).complete
        assert SectorTimes(s1=29.4, s2=38.1, s3=24.0).complete


class TestDriverLapDerivation:
    def test_records_a_first_lap(self):
        d = DriverState(driver_number=1)
        d.apply_lap(lap(1, duration=91.5, s1=29.4, s2=38.1, s3=24.0))
        assert d.current_lap == 1
        assert d.last_lap_time == 91.5
        assert d.best_lap_time == 91.5
        assert d.last_lap_was_personal_best is True

    def test_a_slower_lap_does_not_replace_the_personal_best(self):
        d = DriverState(driver_number=1)
        d.apply_lap(lap(1, duration=91.5))
        d.apply_lap(lap(2, duration=92.8, offset_s=91.5))
        assert d.best_lap_time == 91.5
        assert d.best_lap_number == 1
        assert d.last_lap_time == 92.8
        assert d.last_lap_was_personal_best is False

    def test_a_faster_lap_replaces_the_personal_best(self):
        d = DriverState(driver_number=1)
        d.apply_lap(lap(1, duration=91.5))
        d.apply_lap(lap(2, duration=90.2, offset_s=91.5))
        assert (d.best_lap_time, d.best_lap_number) == (90.2, 2)
        assert d.last_lap_was_personal_best is True

    def test_accumulates_sectors_as_they_arrive_for_one_lap(self):
        """The realistic sequence: upstream publishes the same lap three times."""
        d = DriverState(driver_number=1)
        d.apply_lap(lap(5, s1=29.4))
        assert d.current_sectors.as_tuple() == (29.4, None, None)
        d.apply_lap(lap(5, s1=29.4, s2=38.1))
        assert d.current_sectors.as_tuple() == (29.4, 38.1, None)
        d.apply_lap(lap(5, s1=29.4, s2=38.1, s3=24.0, duration=91.5))
        assert d.current_sectors.complete

    def test_rolls_the_previous_lap_into_last_sectors_on_a_new_lap(self):
        d = DriverState(driver_number=1)
        d.apply_lap(lap(5, s1=29.4, s2=38.1, s3=24.0, duration=91.5))
        d.apply_lap(lap(6, s1=29.1, offset_s=91.5))
        assert d.last_sectors.as_tuple() == (29.4, 38.1, 24.0)
        assert d.current_sectors.as_tuple() == (29.1, None, None)
        assert d.current_lap == 6

    def test_best_sectors_are_tracked_independently_of_the_best_lap(self):
        """A driver's best sectors can come from different laps than their best lap."""
        d = DriverState(driver_number=1)
        d.apply_lap(lap(1, duration=91.5, s1=29.4, s2=38.1, s3=24.0))
        d.apply_lap(lap(2, duration=92.0, s1=29.0, s2=38.5, s3=24.5, offset_s=91.5))
        assert d.best_sectors.s1 == 29.0   # from lap 2
        assert d.best_sectors.s2 == 38.1   # from lap 1
        assert d.best_lap_time == 91.5     # lap 1 is still the best lap

    def test_a_lap_without_a_duration_leaves_the_best_untouched(self):
        """An in-progress lap has no total time yet."""
        d = DriverState(driver_number=1)
        d.apply_lap(lap(1, duration=91.5))
        d.apply_lap(lap(2, s1=29.0, offset_s=91.5))
        assert d.best_lap_time == 91.5
        assert d.last_lap_time == 91.5

    def test_never_regresses_the_lap_counter(self):
        """Events can arrive slightly out of order; the lap number must not go back."""
        d = DriverState(driver_number=1)
        d.apply_lap(lap(7, duration=91.0))
        d.apply_lap(lap(6, duration=92.0))
        assert d.current_lap == 7


class TestDriverTimingDerivation:
    def test_position_and_gap_arrive_from_different_events_without_erasing(self):
        """`/position` carries no gap and `/intervals` carries no position;
        applying either must not clear what the other set."""
        d = DriverState(driver_number=1)
        d.apply_timing(ev(EventType.TIMING, TimingUpdate(position=3)))
        d.apply_timing(ev(EventType.TIMING, TimingUpdate(gap_to_leader=2.104), offset_s=0.5))
        assert d.position == 3
        assert d.gap_to_leader == 2.104

    def test_a_lapped_car_reports_laps_down_and_clears_the_seconds_gap(self):
        d = DriverState(driver_number=1)
        d.apply_timing(ev(EventType.TIMING, TimingUpdate(gap_to_leader=45.0)))
        d.apply_timing(ev(EventType.TIMING, TimingUpdate(laps_down=1), offset_s=1))
        assert d.laps_down == 1
        assert d.gap_to_leader is None

    def test_freshness_never_steps_backwards(self):
        d = DriverState(driver_number=1)
        d.apply_telemetry(ev(EventType.CAR_TELEMETRY, CarTelemetry(speed=300), offset_s=10))
        d.apply_telemetry(ev(EventType.CAR_TELEMETRY, CarTelemetry(speed=250), offset_s=5))
        assert d.last_update == T0 + timedelta(seconds=10)


class TestDriverTelemetry:
    def test_holds_the_latest_sample_only(self):
        d = DriverState(driver_number=1)
        d.apply_telemetry(ev(EventType.CAR_TELEMETRY, CarTelemetry(speed=300, n_gear=7)))
        d.apply_telemetry(
            ev(EventType.CAR_TELEMETRY, CarTelemetry(speed=312, n_gear=8), offset_s=0.27)
        )
        assert (d.speed, d.n_gear) == (312, 8)
        assert d.telemetry_samples == 2

    def test_an_unset_channel_stays_none(self):
        d = DriverState(driver_number=1)
        d.apply_telemetry(ev(EventType.CAR_TELEMETRY, CarTelemetry(speed=300)))
        assert d.rpm is None

    def test_tracks_position_for_the_circuit_map(self):
        d = DriverState(driver_number=1)
        d.apply_position(ev(EventType.POSITION, Position(x=218.0, y=-3473.0)))
        assert (d.x, d.y) == (218.0, -3473.0)


class TestSessionState:
    def test_starts_with_nothing_known(self):
        """Every derived figure is None until an event supplies it."""
        s = SessionState(session_id=SESSION)
        assert s.session_best_lap is None
        assert s.current_lap is None
        assert s.track_temperature is None
        assert s.drivers == {}

    def test_session_best_is_the_fastest_across_all_drivers(self):
        s = SessionState(session_id=SESSION)
        s.apply(lap(1, duration=91.5, driver=1))
        s.apply(lap(1, duration=90.8, driver=16))
        s.apply(lap(1, duration=92.1, driver=4))
        assert s.session_best_lap == 90.8
        assert s.session_best_lap_driver == 16

    def test_session_best_sectors_can_come_from_different_drivers(self):
        s = SessionState(session_id=SESSION)
        s.apply(lap(1, s1=29.4, s2=38.0, s3=24.5, duration=91.9, driver=1))
        s.apply(lap(1, s1=29.1, s2=38.4, s3=24.1, duration=91.6, driver=16))
        assert s.session_best_sectors.as_tuple() == (29.1, 38.0, 24.1)
        assert s.session_best_sector_drivers == {"s1": 16, "s2": 1, "s3": 16}

    def test_session_lap_counter_follows_the_leader(self):
        s = SessionState(session_id=SESSION)
        s.apply(lap(38, duration=91.0, driver=1))
        s.apply(lap(37, duration=92.0, driver=16))
        assert s.current_lap == 38

    def test_applies_weather_without_a_driver(self):
        s = SessionState(session_id=SESSION)
        s.apply(
            Event(
                event_type=EventType.WEATHER,
                source=EventSource.REPLAY,
                session_id=SESSION,
                timestamp=T0,
                payload=Weather(air_temperature=18.9, track_temperature=26.5),
            )
        )
        assert s.track_temperature == 26.5
        assert s.drivers == {}  # weather creates no phantom driver

    def test_applies_session_metadata(self):
        s = SessionState(session_id=SESSION)
        s.apply(
            Event(
                event_type=EventType.SESSION_META,
                source=EventSource.REPLAY,
                session_id=SESSION,
                timestamp=T0,
                payload=SessionMeta(
                    session_name="Race",
                    session_type="Race",
                    circuit_short_name="Sakhir",
                    total_laps=57,
                ),
            )
        )
        assert (s.session_name, s.circuit, s.total_laps) == ("Race", "Sakhir", 57)

    def test_leaderboard_is_ordered_by_position(self):
        s = SessionState(session_id=SESSION)
        for driver, pos in ((4, 3), (1, 1), (16, 2)):
            s.apply(ev(EventType.TIMING, TimingUpdate(position=pos), driver=driver))
        assert [d.driver_number for d in s.leaderboard()] == [1, 16, 4]

    def test_a_car_without_a_position_sorts_last_rather_than_disappearing(self):
        """A car in the pits is still in the session."""
        s = SessionState(session_id=SESSION)
        s.apply(ev(EventType.TIMING, TimingUpdate(position=1), driver=1))
        s.apply(ev(EventType.CAR_TELEMETRY, CarTelemetry(speed=0), driver=77))
        order = [d.driver_number for d in s.leaderboard()]
        assert order == [1, 77]
        assert len(order) == 2

    def test_applying_the_same_lap_twice_is_idempotent_in_effect(self):
        """At-least-once delivery means this happens routinely."""
        s = SessionState(session_id=SESSION)
        e = lap(5, duration=91.5, s1=29.4, s2=38.1, s3=24.0)
        s.apply(e)
        snapshot = s.drivers[1].as_dict()
        s.apply(e)
        after = s.drivers[1].as_dict()
        # telemetry_samples is a counter, not derived state; everything that
        # feeds the timing tower must be unchanged.
        snapshot.pop("telemetry_samples"), after.pop("telemetry_samples")
        assert snapshot == after

    def test_ignores_an_event_type_that_carries_no_timing_state(self):
        from racestream_common.schemas import RaceControl

        s = SessionState(session_id=SESSION)
        s.apply(
            Event(
                event_type=EventType.RACE_CONTROL,
                source=EventSource.REPLAY,
                session_id=SESSION,
                timestamp=T0,
                payload=RaceControl(message="SAFETY CAR DEPLOYED"),
            )
        )
        assert s.events_applied == 1
        assert s.drivers == {}

    def test_serialises_to_a_shape_the_frontend_can_consume(self):
        s = SessionState(session_id=SESSION)
        s.apply(lap(1, duration=91.5, s1=29.4, s2=38.1, s3=24.0))
        s.apply(ev(EventType.TIMING, TimingUpdate(position=1)))
        s.apply(ev(EventType.CAR_TELEMETRY, CarTelemetry(speed=312, n_gear=8)))

        payload = s.as_dict()
        assert payload["session_id"] == SESSION
        assert payload["session_best_lap"] == 91.5
        driver = payload["drivers"][0]
        assert driver["position"] == 1
        assert driver["telemetry"]["speed"] == 312
        assert driver["sectors"]["current"] == [29.4, 38.1, 24.0]
        # Unset channels serialise as null, never as 0.
        assert driver["telemetry"]["rpm"] is None
