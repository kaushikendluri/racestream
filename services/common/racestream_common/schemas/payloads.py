"""Typed payloads carried inside :class:`~racestream_common.schemas.Event`.

Field names and units follow the OpenF1 public API so that a reader can line a
payload up against the upstream response without a translation table. Where we
rename, the docstring says what the original field was.

Every payload is ``extra="forbid"``: an unexpected upstream field is a contract
change we want to see in the dead-letter metrics, not silently absorb.
"""

from __future__ import annotations

from datetime import datetime
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field


class _Payload(BaseModel):
    model_config = ConfigDict(extra="forbid")


class CarTelemetry(_Payload):
    """A car-data sample. OpenF1 ``/car_data``, roughly 3.7 Hz per driver."""

    kind: Literal["car_telemetry"] = "car_telemetry"

    speed: int | None = Field(default=None, ge=0, le=400, description="km/h")
    throttle: int | None = Field(default=None, ge=0, le=100, description="percent")
    brake: int | None = Field(
        default=None, ge=0, le=100, description="percent; upstream reports 0 or 100"
    )
    n_gear: int | None = Field(default=None, ge=0, le=8)
    rpm: int | None = Field(default=None, ge=0, le=20000)
    drs: int | None = Field(
        default=None,
        ge=0,
        le=14,
        description="Raw OpenF1 DRS code; see drs_open for the interpretation.",
    )

    @property
    def drs_open(self) -> bool | None:
        """Whether DRS is open.

        OpenF1 documents 0/1/8 as closed-ish and 10/12/14 as open; 2, 3 and 9
        are undocumented, so they resolve to ``None`` rather than a guess.
        """
        if self.drs is None:
            return None
        if self.drs in (0, 1, 8):
            return False
        if self.drs in (10, 12, 14):
            return True
        return None


class Position(_Payload):
    """Track location in the circuit's own coordinate frame. OpenF1 ``/location``.

    Units are undocumented upstream but are consistent within a circuit, so the
    frontend normalises against the session's observed bounding box rather than
    assuming metres.
    """

    kind: Literal["position"] = "position"

    x: float
    y: float
    z: float | None = None


class TimingUpdate(_Payload):
    """Classification and gaps. OpenF1 ``/position`` joined with ``/intervals``."""

    kind: Literal["timing"] = "timing"

    position: int | None = Field(default=None, ge=1, le=30)
    gap_to_leader: float | None = Field(
        default=None, description="Seconds; None when lapped or not yet known."
    )
    interval: float | None = Field(
        default=None, description="Seconds to the car ahead."
    )
    laps_down: int | None = Field(
        default=None, ge=0, description="Set when the gap is reported as '+N LAP'."
    )


class LapCompleted(_Payload):
    """A completed lap. OpenF1 ``/laps``."""

    kind: Literal["lap"] = "lap"

    lap_number: int = Field(ge=1)
    lap_duration: float | None = Field(default=None, gt=0, description="seconds")
    duration_sector_1: float | None = Field(default=None, gt=0)
    duration_sector_2: float | None = Field(default=None, gt=0)
    duration_sector_3: float | None = Field(default=None, gt=0)
    i1_speed: int | None = Field(default=None, ge=0, le=400)
    i2_speed: int | None = Field(default=None, ge=0, le=400)
    st_speed: int | None = Field(default=None, ge=0, le=400, description="speed trap")
    is_pit_out_lap: bool = False
    date_start: datetime | None = None


class Weather(_Payload):
    """Trackside weather. OpenF1 ``/weather``, one sample per minute."""

    kind: Literal["weather"] = "weather"

    air_temperature: float | None = None
    track_temperature: float | None = None
    humidity: float | None = Field(default=None, ge=0, le=100)
    pressure: float | None = Field(default=None, gt=0, description="mbar")
    rainfall: int | None = Field(default=None, ge=0, le=1)
    wind_speed: float | None = Field(default=None, ge=0, description="m/s")
    wind_direction: int | None = Field(default=None, ge=0, le=360)


class RaceControl(_Payload):
    """A race-control message. OpenF1 ``/race_control``. Drives replay markers."""

    kind: Literal["race_control"] = "race_control"

    category: str | None = Field(default=None, max_length=64)
    flag: str | None = Field(default=None, max_length=32)
    scope: str | None = Field(default=None, max_length=32)
    sector: int | None = Field(default=None, ge=1, le=30)
    lap_number: int | None = Field(default=None, ge=0)
    message: str = Field(max_length=1024)


class SessionMeta(_Payload):
    """Session identity, emitted once at the head of a stream."""

    kind: Literal["session_meta"] = "session_meta"

    session_name: str = Field(max_length=128)
    session_type: str = Field(max_length=64)
    circuit_short_name: str | None = Field(default=None, max_length=128)
    country_name: str | None = Field(default=None, max_length=128)
    location: str | None = Field(default=None, max_length=128)
    year: int | None = Field(default=None, ge=1950, le=2100)
    date_start: datetime | None = None
    date_end: datetime | None = None
    total_laps: int | None = Field(default=None, ge=0)


PAYLOAD_MODELS: dict[str, type[_Payload]] = {
    m.model_fields["kind"].default: m
    for m in (
        CarTelemetry,
        Position,
        TimingUpdate,
        LapCompleted,
        Weather,
        RaceControl,
        SessionMeta,
    )
}
"""Lookup from ``event_type`` to payload model, used by validation and tests."""
