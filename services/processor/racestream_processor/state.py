"""Derived session state.

What can and cannot be derived in a scaled-out consumer
------------------------------------------------------
Kafka guarantees ordering within a partition, and events are keyed
``session:driver``. So **per-car** derivation - this driver's last lap, personal
best, sector deltas, current stint - is correct in any number of consumer
instances, because one car's events always land on one partition.

**Cross-car** derivation is not. "Session best lap" requires comparing every
driver, and with two processor instances each owning three partitions, neither
one sees all twenty cars. An instance computing a session best from its own
partitions would produce a confidently wrong number.

The resolution is to split the work by what it needs, not by convenience:

* High-volume per-car channels (``telemetry.v1``, ``position.v1``) are consumed
  by a group that scales horizontally and derives only per-car facts.
* Session-wide aggregation runs on ``timing.v1``, which is deliberately low
  volume (laps and classification, not 3.9 Hz telemetry) and is therefore
  comfortably handled by a single consumer that sees every car.

That is also why ``timing.v1`` has three partitions rather than six, and why
``session.v1`` has one. See docs/architecture.md.

State lifetime
--------------
This state is in-memory and rebuildable. It is a *materialised view* of the
event log, not a system of record: TimescaleDB holds the durable data, and on
restart the state is rebuilt by querying it. Nothing is lost by dropping it.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime

from racestream_common.schemas import Event, EventType


@dataclass
class SectorTimes:
    """Three sectors, each independently optional.

    Sectors arrive one at a time as a car passes each split, so a partially
    filled lap is the normal case rather than an error.
    """

    s1: float | None = None
    s2: float | None = None
    s3: float | None = None

    def as_tuple(self) -> tuple[float | None, float | None, float | None]:
        return (self.s1, self.s2, self.s3)

    @property
    def complete(self) -> bool:
        return all(s is not None for s in self.as_tuple())

    def merge(self, other: "SectorTimes") -> "SectorTimes":
        """Later values win, but a later ``None`` never erases a known value."""
        return SectorTimes(
            s1=other.s1 if other.s1 is not None else self.s1,
            s2=other.s2 if other.s2 is not None else self.s2,
            s3=other.s3 if other.s3 is not None else self.s3,
        )


@dataclass
class DriverState:
    """Everything the timing tower shows for one car.

    Every field starts as ``None`` and only becomes a number when an event has
    actually supplied one. A driver who has not yet set a lap has
    ``best_lap=None``, which the UI renders as a dash - not as 0.000.
    """

    driver_number: int

    # Classification
    position: int | None = None
    gap_to_leader: float | None = None
    interval: float | None = None
    laps_down: int | None = None

    # Lap progression
    current_lap: int | None = None
    last_lap_time: float | None = None
    best_lap_time: float | None = None
    best_lap_number: int | None = None

    # Sectors
    current_sectors: SectorTimes = field(default_factory=SectorTimes)
    last_sectors: SectorTimes = field(default_factory=SectorTimes)
    best_sectors: SectorTimes = field(default_factory=SectorTimes)

    # Latest telemetry sample. Held as a single snapshot rather than a series:
    # the tower shows the current value, and history is a database query.
    speed: int | None = None
    throttle: int | None = None
    brake: int | None = None
    n_gear: int | None = None
    rpm: int | None = None
    drs: int | None = None

    # Track position, for the circuit map.
    x: float | None = None
    y: float | None = None

    # Derived flags. `None` means "not yet determined", which is distinct from
    # False ("determined, and it is not a best").
    last_lap_was_personal_best: bool | None = None

    last_update: datetime | None = None
    telemetry_samples: int = 0

    def apply_telemetry(self, event: Event) -> None:
        p = event.payload
        self.speed = p.speed
        self.throttle = p.throttle
        self.brake = p.brake
        self.n_gear = p.n_gear
        self.rpm = p.rpm
        self.drs = p.drs
        self.telemetry_samples += 1
        self._touch(event.timestamp)

    def apply_position(self, event: Event) -> None:
        self.x = event.payload.x
        self.y = event.payload.y
        self._touch(event.timestamp)

    def apply_timing(self, event: Event) -> None:
        """Classification and gaps.

        Fields are applied only when present. A timing event from the
        ``/position`` endpoint carries a position and no gap; one from
        ``/intervals`` carries gaps and no position. Overwriting with the
        absent field would make each message erase what the other just set.
        """
        p = event.payload
        if p.position is not None:
            self.position = p.position
        if p.gap_to_leader is not None:
            self.gap_to_leader = p.gap_to_leader
        if p.interval is not None:
            self.interval = p.interval
        if p.laps_down is not None:
            self.laps_down = p.laps_down
            # A lapped car has no meaningful seconds gap.
            self.gap_to_leader = None
        self._touch(event.timestamp)

    def apply_lap(self, event: Event) -> None:
        """Record a lap and update personal bests.

        Upstream republishes a lap as its sectors complete, so this runs several
        times for the same lap. It must therefore be idempotent in effect.
        """
        p = event.payload
        incoming = SectorTimes(p.duration_sector_1, p.duration_sector_2, p.duration_sector_3)

        if self.current_lap is not None and p.lap_number > self.current_lap:
            # A new lap has started; what was accumulating becomes the last lap.
            self.last_sectors = self.current_sectors
            self.current_sectors = incoming
        elif self.current_lap is not None and p.lap_number == self.current_lap:
            self.current_sectors = self.current_sectors.merge(incoming)
        else:
            self.current_sectors = incoming

        self.current_lap = max(self.current_lap or 0, p.lap_number)

        for attr in ("s1", "s2", "s3"):
            value = getattr(incoming, attr)
            best = getattr(self.best_sectors, attr)
            if value is not None and (best is None or value < best):
                setattr(self.best_sectors, attr, value)

        if p.lap_duration is not None:
            self.last_lap_time = p.lap_duration
            if self.best_lap_time is None or p.lap_duration < self.best_lap_time:
                self.best_lap_time = p.lap_duration
                self.best_lap_number = p.lap_number
                self.last_lap_was_personal_best = True
            elif (
                p.lap_number == self.best_lap_number
                and p.lap_duration == self.best_lap_time
            ):
                # Re-delivery of the lap that already holds the best. Comparing
                # it against itself gives `not faster`, which would clear the
                # flag and drop the personal-best highlight off the tower. With
                # at-least-once delivery this happens routinely, so the lap that
                # set the best is recognised rather than re-compared.
                self.last_lap_was_personal_best = True
            else:
                self.last_lap_was_personal_best = False

        self._touch(event.timestamp)

    def _touch(self, when: datetime) -> None:
        # Events can arrive slightly out of order across channels; the freshness
        # indicator should reflect the newest event seen, never step backwards.
        if self.last_update is None or when > self.last_update:
            self.last_update = when

    def as_dict(self) -> dict:
        return {
            "driver_number": self.driver_number,
            "position": self.position,
            "gap_to_leader": self.gap_to_leader,
            "interval": self.interval,
            "laps_down": self.laps_down,
            "current_lap": self.current_lap,
            "last_lap_time": self.last_lap_time,
            "best_lap_time": self.best_lap_time,
            "best_lap_number": self.best_lap_number,
            "sectors": {
                "current": list(self.current_sectors.as_tuple()),
                "last": list(self.last_sectors.as_tuple()),
                "best": list(self.best_sectors.as_tuple()),
            },
            "telemetry": {
                "speed": self.speed,
                "throttle": self.throttle,
                "brake": self.brake,
                "n_gear": self.n_gear,
                "rpm": self.rpm,
                "drs": self.drs,
            },
            "track_position": {"x": self.x, "y": self.y},
            "last_lap_was_personal_best": self.last_lap_was_personal_best,
            "last_update": self.last_update.isoformat() if self.last_update else None,
            "telemetry_samples": self.telemetry_samples,
        }


@dataclass
class SessionState:
    """Session-wide derived state.

    Correct only in a consumer that sees every partition of the topics it
    aggregates. See the module docstring: this runs on ``timing.v1``, which is
    sized so one consumer can keep up with all twenty cars.
    """

    session_id: int
    drivers: dict[int, DriverState] = field(default_factory=dict)

    session_best_lap: float | None = None
    session_best_lap_driver: int | None = None
    session_best_sectors: SectorTimes = field(default_factory=SectorTimes)
    session_best_sector_drivers: dict[str, int | None] = field(
        default_factory=lambda: {"s1": None, "s2": None, "s3": None}
    )

    current_lap: int | None = None
    total_laps: int | None = None
    session_name: str | None = None
    session_type: str | None = None
    circuit: str | None = None

    air_temperature: float | None = None
    track_temperature: float | None = None
    rainfall: int | None = None

    last_event_at: datetime | None = None
    events_applied: int = 0

    def driver(self, number: int) -> DriverState:
        state = self.drivers.get(number)
        if state is None:
            state = DriverState(driver_number=number)
            self.drivers[number] = state
        return state

    def apply(self, event: Event) -> None:
        """Route one event into the state. Unknown types are ignored, not fatal."""
        self.events_applied += 1
        if self.last_event_at is None or event.timestamp > self.last_event_at:
            self.last_event_at = event.timestamp

        et = event.event_type

        if et is EventType.WEATHER:
            p = event.payload
            if p.air_temperature is not None:
                self.air_temperature = p.air_temperature
            if p.track_temperature is not None:
                self.track_temperature = p.track_temperature
            if p.rainfall is not None:
                self.rainfall = p.rainfall
            return

        if et is EventType.SESSION_META:
            p = event.payload
            self.session_name = p.session_name
            self.session_type = p.session_type
            self.circuit = p.circuit_short_name
            if p.total_laps is not None:
                self.total_laps = p.total_laps
            return

        if et is EventType.RACE_CONTROL:
            return  # persisted and streamed, but holds no timing state

        if event.driver_id is None:
            return

        driver = self.driver(event.driver_id)

        if et is EventType.CAR_TELEMETRY:
            driver.apply_telemetry(event)
        elif et is EventType.POSITION:
            driver.apply_position(event)
        elif et is EventType.TIMING:
            driver.apply_timing(event)
        elif et is EventType.LAP:
            driver.apply_lap(event)
            self._update_session_bests(driver)
            # The session's lap counter is the leader's, which is the highest
            # lap any car has started.
            if driver.current_lap is not None:
                self.current_lap = max(self.current_lap or 0, driver.current_lap)

    def _update_session_bests(self, driver: DriverState) -> None:
        if driver.best_lap_time is not None and (
            self.session_best_lap is None or driver.best_lap_time < self.session_best_lap
        ):
            self.session_best_lap = driver.best_lap_time
            self.session_best_lap_driver = driver.driver_number

        for attr in ("s1", "s2", "s3"):
            value = getattr(driver.best_sectors, attr)
            best = getattr(self.session_best_sectors, attr)
            if value is not None and (best is None or value < best):
                setattr(self.session_best_sectors, attr, value)
                self.session_best_sector_drivers[attr] = driver.driver_number

    def leaderboard(self) -> list[DriverState]:
        """Drivers in classification order.

        Cars without a position sort last rather than being dropped: a car in
        the pits is still in the session, and omitting it from the tower would
        be a worse lie than showing it unplaced.
        """
        return sorted(
            self.drivers.values(),
            key=lambda d: (d.position is None, d.position or 0, d.driver_number),
        )

    def as_dict(self) -> dict:
        return {
            "session_id": self.session_id,
            "session_name": self.session_name,
            "session_type": self.session_type,
            "circuit": self.circuit,
            "current_lap": self.current_lap,
            "total_laps": self.total_laps,
            "session_best_lap": self.session_best_lap,
            "session_best_lap_driver": self.session_best_lap_driver,
            "session_best_sectors": list(self.session_best_sectors.as_tuple()),
            "session_best_sector_drivers": dict(self.session_best_sector_drivers),
            "weather": {
                "air_temperature": self.air_temperature,
                "track_temperature": self.track_temperature,
                "rainfall": self.rainfall,
            },
            "last_event_at": self.last_event_at.isoformat() if self.last_event_at else None,
            "events_applied": self.events_applied,
            "drivers": [d.as_dict() for d in self.leaderboard()],
        }
