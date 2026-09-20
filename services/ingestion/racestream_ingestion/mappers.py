"""Upstream records -> RaceStream events.

This is the boundary where untrusted external data becomes a typed event, and
it is the only place in the system that knows OpenF1's field names.

Two rules hold throughout:

1. **A record that cannot be mapped is dropped and counted, never guessed at.**
   A missing timestamp, an out-of-range speed, a null where a key is required -
   all produce a counted drop, not a substituted default.
2. **Absence is preserved.** Where upstream omits a channel, the payload field
   stays ``None`` and travels all the way to an em-dash on screen.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Callable, Iterable

from racestream_common.obs import METRICS, get_logger
from racestream_common.obs.metrics import utcnow
from racestream_common.schemas import (
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

log = get_logger(__name__)


class MappingError(ValueError):
    """A record could not be turned into a valid event."""


# ---------------------------------------------------------------- primitives


def parse_timestamp(value: Any) -> datetime:
    """Parse an OpenF1 date into an aware UTC datetime.

    Upstream is inconsistent: some endpoints include an offset
    (``2024-03-02T15:03:42.341000+00:00``), others do not
    (``2024-03-02T14:06:41``). A naive value is treated as UTC, which is what
    the API documents, and is recorded as such rather than guessed per-record.
    """
    if value is None:
        raise MappingError("record has no timestamp")
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, str):
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError as exc:
            raise MappingError(f"unparseable timestamp {value!r}") from exc
    else:
        raise MappingError(f"timestamp has type {type(value).__name__}")

    return parsed.replace(tzinfo=timezone.utc) if parsed.tzinfo is None else parsed.astimezone(timezone.utc)


def _int(value: Any) -> int | None:
    """Coerce to int, returning None rather than raising.

    Used only for optional telemetry channels, where an unusable value and an
    absent one are equally 'unknown'. Required fields never go through here.
    """
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        result = float(value)
    except (TypeError, ValueError):
        return None
    # NaN and infinity survive float() but are not measurements.
    return result if result == result and abs(result) != float("inf") else None


def _require_int(record: dict, field: str) -> int:
    value = _int(record.get(field))
    if value is None:
        raise MappingError(f"required field {field!r} is missing or not an integer")
    return value


def _clamp_or_none(value: int | None, low: int, high: int, field: str) -> int | None:
    """Reject an out-of-range reading instead of clamping it.

    Clamping would turn a sensor glitch into a plausible number. A value outside
    physical limits is not a measurement, so it becomes ``None`` and is counted.
    """
    if value is None:
        return None
    if low <= value <= high:
        return value
    METRICS.messages_dropped.labels(reason=f"out_of_range_{field}", stage="ingest").inc()
    return None


def _new_event(
    event_type: EventType,
    session_id: int,
    timestamp: datetime,
    payload,
    driver_id: int | None,
    source: EventSource,
    source_ts: datetime | None = None,
) -> Event:
    """Build an envelope, stamping the two timestamps we can honestly stamp."""
    trace = PipelineTrace(source_ts=source_ts or timestamp).stamp("ingest_ts", utcnow())
    return Event(
        event_type=event_type,
        source=source,
        session_id=session_id,
        driver_id=driver_id,
        timestamp=timestamp,
        payload=payload,
        trace=trace,
    )


# ------------------------------------------------------------------ mappers


def map_car_telemetry(
    record: dict[str, Any], source: EventSource = EventSource.LIVE
) -> Event:
    """``/car_data`` -> a ``car_telemetry`` event. ~3.9 Hz per driver, measured."""
    ts = parse_timestamp(record.get("date"))
    payload = CarTelemetry(
        speed=_clamp_or_none(_int(record.get("speed")), 0, 400, "speed"),
        throttle=_clamp_or_none(_int(record.get("throttle")), 0, 100, "throttle"),
        brake=_clamp_or_none(_int(record.get("brake")), 0, 100, "brake"),
        n_gear=_clamp_or_none(_int(record.get("n_gear")), 0, 8, "n_gear"),
        rpm=_clamp_or_none(_int(record.get("rpm")), 0, 20000, "rpm"),
        drs=_clamp_or_none(_int(record.get("drs")), 0, 14, "drs"),
    )
    return _new_event(
        EventType.CAR_TELEMETRY,
        _require_int(record, "session_key"),
        ts,
        payload,
        _require_int(record, "driver_number"),
        source,
    )


def map_location(record: dict[str, Any], source: EventSource = EventSource.LIVE) -> Event:
    """``/location`` -> a ``position`` event.

    x and y are required: a track map cannot plot a car without both. z is
    optional because it is not used for the 2D map.
    """
    ts = parse_timestamp(record.get("date"))
    x, y = _float(record.get("x")), _float(record.get("y"))
    if x is None or y is None:
        raise MappingError("location record has no usable x/y")
    # Upstream emits (0, 0) for a car that is not on track. Plotting it would
    # park a phantom car at the circuit origin.
    if x == 0.0 and y == 0.0:
        raise MappingError("location is the null island placeholder (0, 0)")

    return _new_event(
        EventType.POSITION,
        _require_int(record, "session_key"),
        ts,
        Position(x=x, y=y, z=_float(record.get("z"))),
        _require_int(record, "driver_number"),
        source,
    )


def map_lap(record: dict[str, Any], source: EventSource = EventSource.LIVE) -> Event:
    """``/laps`` -> a ``lap`` event.

    ``date_start`` is the authoritative time and part of the primary key. A lap
    without one cannot be stored idempotently, so it is rejected rather than
    given a synthetic timestamp.
    """
    start = parse_timestamp(record.get("date_start"))
    payload = LapCompleted(
        lap_number=_require_int(record, "lap_number"),
        lap_duration=_positive(_float(record.get("lap_duration"))),
        duration_sector_1=_positive(_float(record.get("duration_sector_1"))),
        duration_sector_2=_positive(_float(record.get("duration_sector_2"))),
        duration_sector_3=_positive(_float(record.get("duration_sector_3"))),
        i1_speed=_clamp_or_none(_int(record.get("i1_speed")), 0, 400, "i1_speed"),
        i2_speed=_clamp_or_none(_int(record.get("i2_speed")), 0, 400, "i2_speed"),
        st_speed=_clamp_or_none(_int(record.get("st_speed")), 0, 400, "st_speed"),
        is_pit_out_lap=bool(record.get("is_pit_out_lap")),
        date_start=start,
    )
    return _new_event(
        EventType.LAP,
        _require_int(record, "session_key"),
        start,
        payload,
        _require_int(record, "driver_number"),
        source,
    )


def _positive(value: float | None) -> float | None:
    """A duration of zero or less is not a lap time."""
    return value if value is not None and value > 0 else None


def map_weather(record: dict[str, Any], source: EventSource = EventSource.LIVE) -> Event:
    """``/weather`` -> a ``weather`` event. One sample per minute."""
    ts = parse_timestamp(record.get("date"))
    payload = Weather(
        air_temperature=_float(record.get("air_temperature")),
        track_temperature=_float(record.get("track_temperature")),
        humidity=_bounded_float(_float(record.get("humidity")), 0, 100),
        pressure=_positive(_float(record.get("pressure"))),
        rainfall=_clamp_or_none(_int(record.get("rainfall")), 0, 1, "rainfall"),
        wind_speed=_bounded_float(_float(record.get("wind_speed")), 0, 200),
        wind_direction=_clamp_or_none(
            _int(record.get("wind_direction")), 0, 360, "wind_direction"
        ),
    )
    return _new_event(
        EventType.WEATHER,
        _require_int(record, "session_key"),
        ts,
        payload,
        None,
        source,
    )


def _bounded_float(value: float | None, low: float, high: float) -> float | None:
    if value is None:
        return None
    return value if low <= value <= high else None


def map_race_control(
    record: dict[str, Any], source: EventSource = EventSource.LIVE
) -> Event:
    """``/race_control`` -> a ``race_control`` event. Drives the replay markers.

    Note: upstream also carries ``qualifying_phase``, which is not modelled -
    the replay timeline does not use it, and an unused field is a field that can
    drift unnoticed.
    """
    ts = parse_timestamp(record.get("date"))
    message = record.get("message")
    if not message or not isinstance(message, str):
        raise MappingError("race control record has no message")

    payload = RaceControl(
        category=_short_str(record.get("category"), 64),
        flag=_short_str(record.get("flag"), 32),
        scope=_short_str(record.get("scope"), 32),
        sector=_clamp_or_none(_int(record.get("sector")), 1, 30, "sector"),
        lap_number=_int(record.get("lap_number")),
        message=message[:1024],
    )
    return _new_event(
        EventType.RACE_CONTROL,
        _require_int(record, "session_key"),
        ts,
        payload,
        _int(record.get("driver_number")),  # optional: not every message names a driver
        source,
    )


def _short_str(value: Any, limit: int) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text[:limit] if text else None


def map_position(record: dict[str, Any], source: EventSource = EventSource.LIVE) -> Event:
    """``/position`` (classification) -> a ``timing`` event.

    Not to be confused with ``/location``. OpenF1 names the track-coordinate
    endpoint ``location`` and the classification endpoint ``position``; this
    system calls the former ``position`` and the latter ``timing``, because that
    is what each one is for.
    """
    ts = parse_timestamp(record.get("date"))
    return _new_event(
        EventType.TIMING,
        _require_int(record, "session_key"),
        ts,
        TimingUpdate(position=_clamp_or_none(_int(record.get("position")), 1, 30, "position")),
        _require_int(record, "driver_number"),
        source,
    )


def map_interval(record: dict[str, Any], source: EventSource = EventSource.LIVE) -> Event:
    """``/intervals`` -> a ``timing`` event carrying gaps.

    Gaps arrive either as seconds or as the string ``"+1 LAP"``. Both are
    represented without loss: a lapped car gets ``laps_down`` set and
    ``gap_to_leader`` left ``None``, because there is no meaningful number of
    seconds to report.
    """
    ts = parse_timestamp(record.get("date"))
    gap, laps_down = _parse_gap(record.get("gap_to_leader"))
    interval, _ = _parse_gap(record.get("interval"))

    return _new_event(
        EventType.TIMING,
        _require_int(record, "session_key"),
        ts,
        TimingUpdate(gap_to_leader=gap, interval=interval, laps_down=laps_down),
        _require_int(record, "driver_number"),
        source,
    )


def _parse_gap(value: Any) -> tuple[float | None, int | None]:
    """Return (seconds, laps_down). Exactly one is meaningful for a given value."""
    if value is None:
        return None, None
    if isinstance(value, (int, float)):
        return _float(value), None
    text = str(value).strip().upper()
    if "LAP" in text:
        digits = "".join(ch for ch in text if ch.isdigit())
        return None, int(digits) if digits else 1
    return _float(text.lstrip("+")), None


def map_session_meta(
    record: dict[str, Any], total_laps: int | None = None, source: EventSource = EventSource.LIVE
) -> Event:
    """``/sessions`` -> a ``session_meta`` event, emitted at the head of a stream."""
    start = record.get("date_start")
    ts = parse_timestamp(start) if start else utcnow()
    payload = SessionMeta(
        session_name=str(record.get("session_name") or "Unknown")[:128],
        session_type=str(record.get("session_type") or "Unknown")[:64],
        circuit_short_name=_short_str(record.get("circuit_short_name"), 128),
        country_name=_short_str(record.get("country_name"), 128),
        location=_short_str(record.get("location"), 128),
        year=_int(record.get("year")),
        date_start=parse_timestamp(start) if start else None,
        date_end=parse_timestamp(record["date_end"]) if record.get("date_end") else None,
        total_laps=total_laps,
    )
    return _new_event(
        EventType.SESSION_META,
        _require_int(record, "session_key"),
        ts,
        payload,
        None,
        source,
    )


# ------------------------------------------------------------- bulk mapping

MAPPERS: dict[str, Callable[..., Event]] = {
    "car_data": map_car_telemetry,
    "location": map_location,
    "laps": map_lap,
    "weather": map_weather,
    "race_control": map_race_control,
    "position": map_position,
    "intervals": map_interval,
}


def map_many(
    endpoint: str,
    records: Iterable[dict[str, Any]],
    source: EventSource = EventSource.LIVE,
) -> tuple[list[Event], int]:
    """Map a batch, isolating per-record failures.

    Returns ``(events, dropped)``. One malformed record must never cost the
    other 22,439 in the window, so failures are counted and skipped.
    """
    mapper = MAPPERS.get(endpoint)
    if mapper is None:
        raise ValueError(f"no mapper registered for endpoint {endpoint!r}")

    events: list[Event] = []
    dropped = 0
    first_error: str | None = None

    for record in records:
        try:
            events.append(mapper(record, source))
        except Exception as exc:
            dropped += 1
            if first_error is None:
                first_error = f"{type(exc).__name__}: {exc}"
            METRICS.messages_dropped.labels(reason="mapping_failed", stage="ingest").inc()

    if dropped:
        # Log once per batch with a count, not once per record: a systematic
        # upstream change would otherwise produce tens of thousands of lines.
        log.warning(
            "ingest.records_dropped",
            endpoint=endpoint,
            dropped=dropped,
            mapped=len(events),
            first_error=first_error,
        )

    if events:
        METRICS.messages_ingested.labels(
            event_type=events[0].event_type.value, source=source.value
        ).inc(len(events))

    return events, dropped
