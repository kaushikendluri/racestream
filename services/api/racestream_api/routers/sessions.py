"""Session metadata and historical queries.

These serve what the WebSocket deliberately does not: reference data that does
not change during a session (the entry list) and history that predates the
client's connection. Pushing either over the stream would mean re-sending
unchanging data on every snapshot.

All queries are parameterised. Pagination limits are bounded so a single request
cannot ask the database for a whole session's telemetry.
"""

from __future__ import annotations

from datetime import datetime

from fastapi import APIRouter, HTTPException, Path, Query, Request

from racestream_api.models import DriverInfo, SessionSummary
from racestream_common.db import Database
from racestream_common.obs import get_logger

router = APIRouter(prefix="/api/sessions", tags=["sessions"])
log = get_logger(__name__)

MAX_TELEMETRY_ROWS = 20_000


def _db(request: Request) -> Database:
    database = request.app.state.racestream.database
    if database is None or not database.connected:
        raise HTTPException(status_code=503, detail="The database is not available.")
    return database


def _to_summary(row) -> SessionSummary:
    return SessionSummary(
        session_id=row["session_id"],
        year=row["year"],
        session_name=row["session_name"],
        session_type=row["session_type"],
        circuit_short_name=row["circuit_short_name"],
        country_name=row["country_name"],
        location=row["location"],
        date_start=row["date_start"],
        date_end=row["date_end"],
        total_laps=row["total_laps"],
        ingest_status=row["ingest_status"],
        ingested_at=row["ingested_at"],
        # Read from the row rather than inferred from an empty query, so the UI
        # can state "telemetry unavailable" as a fact.
        available_channels={
            "car_telemetry": row["has_car_telemetry"],
            "position": row["has_position"],
            "laps": row["has_laps"],
            "weather": row["has_weather"],
            "race_control": row["has_race_control"],
        },
        # Replay needs car positions and timing to be worth watching. A session
        # with only weather is "ingested" but not replayable, and says so.
        replay_available=bool(row["has_car_telemetry"] or row["has_position"])
        and row["ingest_status"] in ("complete", "partial"),
    )


_SESSION_COLUMNS = """
    session_id, year, session_name, session_type, circuit_short_name,
    country_name, location, date_start, date_end, total_laps,
    ingest_status, ingested_at, has_car_telemetry, has_position,
    has_laps, has_weather, has_race_control
"""


@router.get("", response_model=list[SessionSummary], summary="List ingested sessions.")
async def list_sessions(
    request: Request,
    year: int | None = Query(default=None, ge=1950, le=2100),
    session_type: str | None = Query(default=None, max_length=64),
    limit: int = Query(default=50, ge=1, le=200),
    offset: int = Query(default=0, ge=0),
) -> list[SessionSummary]:
    rows = await _db(request).fetch(
        f"""
        SELECT {_SESSION_COLUMNS}
        FROM sessions
        WHERE ($1::int IS NULL OR year = $1)
          AND ($2::text IS NULL OR session_type = $2)
        ORDER BY date_start DESC NULLS LAST, session_id DESC
        LIMIT $3 OFFSET $4
        """,
        year,
        session_type,
        limit,
        offset,
    )
    return [_to_summary(r) for r in rows]


@router.get("/{session_id}", response_model=SessionSummary, summary="One session.")
async def get_session(request: Request, session_id: int = Path(ge=1)) -> SessionSummary:
    row = await _db(request).fetchrow(
        f"SELECT {_SESSION_COLUMNS} FROM sessions WHERE session_id = $1", session_id
    )
    if row is None:
        raise HTTPException(status_code=404, detail=f"Session {session_id} has not been ingested.")
    return _to_summary(row)


@router.get(
    "/{session_id}/drivers",
    response_model=list[DriverInfo],
    summary="Entry list for a session.",
)
async def session_drivers(request: Request, session_id: int = Path(ge=1)) -> list[DriverInfo]:
    """Names, teams and colours.

    Fetched once by the frontend and held in a map. This is reference data: it
    does not change during a session, so re-sending it with every WebSocket
    snapshot would be waste.
    """
    rows = await _db(request).fetch(
        """
        SELECT driver_number, name_acronym, full_name, broadcast_name,
               country_code, team_name, team_colour, headshot_url
        FROM drivers
        WHERE session_id = $1
        ORDER BY driver_number
        """,
        session_id,
    )
    return [DriverInfo(**dict(r)) for r in rows]


@router.get("/{session_id}/laps", summary="Lap times for a session.")
async def session_laps(
    request: Request,
    session_id: int = Path(ge=1),
    driver_number: int | None = Query(default=None, ge=1, le=99),
    limit: int = Query(default=2000, ge=1, le=10000),
) -> dict:
    rows = await _db(request).fetch(
        """
        SELECT driver_number, lap_number, lap_start, lap_duration,
               duration_sector_1, duration_sector_2, duration_sector_3,
               i1_speed, i2_speed, st_speed, is_pit_out_lap
        FROM laps
        WHERE session_id = $1
          AND ($2::int IS NULL OR driver_number = $2)
        ORDER BY driver_number, lap_number
        LIMIT $3
        """,
        session_id,
        driver_number,
        limit,
    )
    return {"session_id": session_id, "count": len(rows), "laps": [dict(r) for r in rows]}


@router.get(
    "/{session_id}/telemetry",
    summary="Car telemetry over a time range.",
)
async def session_telemetry(
    request: Request,
    session_id: int = Path(ge=1),
    driver_number: int = Query(ge=1, le=99),
    start: datetime | None = Query(default=None),
    end: datetime | None = Query(default=None),
    limit: int = Query(default=5000, ge=1, le=MAX_TELEMETRY_ROWS),
) -> dict:
    """Historical telemetry for one driver.

    ``driver_number`` is required and the row limit is capped: an unbounded
    query here would be a request for millions of rows, which is neither
    something the browser can render nor something the database should be
    asked for by accident.
    """
    rows = await _db(request).fetch(
        """
        SELECT sample_time, speed, throttle, brake, n_gear, rpm, drs
        FROM car_telemetry
        WHERE session_id = $1
          AND driver_number = $2
          AND ($3::timestamptz IS NULL OR sample_time >= $3)
          AND ($4::timestamptz IS NULL OR sample_time <= $4)
        ORDER BY sample_time
        LIMIT $5
        """,
        session_id,
        driver_number,
        start,
        end,
        limit,
    )
    return {
        "session_id": session_id,
        "driver_number": driver_number,
        "count": len(rows),
        # Tells the caller the window was clipped, instead of silently
        # returning a partial series that looks complete.
        "truncated": len(rows) == limit,
        "samples": [dict(r) for r in rows],
    }


@router.get("/{session_id}/race-control", summary="Race control messages.")
async def session_race_control(
    request: Request,
    session_id: int = Path(ge=1),
    limit: int = Query(default=500, ge=1, le=2000),
) -> dict:
    rows = await _db(request).fetch(
        """
        SELECT event_time, driver_number, category, flag, scope,
               sector, lap_number, message
        FROM race_control_events
        WHERE session_id = $1
        ORDER BY event_time
        LIMIT $2
        """,
        session_id,
        limit,
    )
    return {"session_id": session_id, "count": len(rows), "events": [dict(r) for r in rows]}
