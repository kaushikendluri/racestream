"""Replay service.

A standalone service with its own small control surface, rather than a mode of
the ingestion service or a thread inside the API.

Why standalone: it has a different lifecycle (on-demand, not continuous), a
different control surface (play/pause/seek/speed), and a different source (the
database, not HTTP). Folding it into another service would put two unrelated
state machines in one process. Keeping it separate also proves the architectural
claim that matters - replay is *just another producer*, so the downstream path
cannot special-case it.

The main API proxies ``/api/replay/*`` here, so the frontend talks to one origin.
"""

from __future__ import annotations

from contextlib import asynccontextmanager
from datetime import datetime
from typing import AsyncIterator

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse, Response
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest
from pydantic import BaseModel, ConfigDict, Field, field_validator

from racestream_common.config import get_settings
from racestream_common.db import Database
from racestream_common.kafka import ensure_topics
from racestream_common.obs import configure_logging, get_logger

from racestream_replay.engine import VALID_SPEEDS, ReplayEngine, ReplayState

settings = get_settings()
configure_logging(
    service="replay",
    level=settings.obs.log_level,
    json_output=settings.obs.log_json,
    environment=settings.obs.environment,
)
log = get_logger(__name__)


class StartRequest(BaseModel):
    """Replay parameters. Validated here so a bad request cannot reach the engine."""

    model_config = ConfigDict(extra="forbid")

    session_id: int = Field(ge=1, description="Session to replay. Must already be ingested.")
    speed: float = Field(default=1.0, description=f"One of {VALID_SPEEDS}.")
    start_at: datetime | None = Field(
        default=None, description="Start from this timestamp instead of the beginning."
    )
    from_lap: int | None = Field(
        default=None, ge=1, le=200, description="Start from this lap. Overrides start_at."
    )

    @field_validator("speed")
    @classmethod
    def _known_speed(cls, v: float) -> float:
        if v not in VALID_SPEEDS:
            raise ValueError(f"speed must be one of {VALID_SPEEDS}")
        return v


class SpeedRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")
    speed: float

    @field_validator("speed")
    @classmethod
    def _known_speed(cls, v: float) -> float:
        if v not in VALID_SPEEDS:
            raise ValueError(f"speed must be one of {VALID_SPEEDS}")
        return v


class SeekRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")
    to: datetime | None = None
    lap: int | None = Field(default=None, ge=1, le=200)


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    database = Database(settings.db)
    await database.connect()
    await ensure_topics(settings.kafka.bootstrap_servers)

    app.state.database = database
    app.state.engine = ReplayEngine(settings, database)
    log.info("replay.service_started")
    try:
        yield
    finally:
        # A replay left running past shutdown would keep publishing into a
        # pipeline whose controller has gone.
        await app.state.engine.stop()
        await database.close()
        log.info("replay.service_stopped")


app = FastAPI(
    title="RaceStream Replay",
    version="0.1.0",
    summary="Replay a stored session through the live streaming pipeline.",
    description=(
        "Publishes stored telemetry to the same Kafka topics as live ingestion, "
        "with `source: replay` so the UI can report what it is showing."
    ),
    lifespan=lifespan,
)


def _engine(request: Request) -> ReplayEngine:
    return request.app.state.engine


@app.post("/replay/start", summary="Start a replay.")
async def start(request: Request, body: StartRequest) -> dict:
    try:
        status = await _engine(request).start(
            session_id=body.session_id,
            speed=body.speed,
            start_at=body.start_at,
            from_lap=body.from_lap,
        )
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except RuntimeError as exc:
        # Already running is a conflict, not a server fault.
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    return status.as_dict()


@app.post("/replay/pause", summary="Pause a running replay.")
async def pause(request: Request) -> dict:
    return _engine(request).pause().as_dict()


@app.post("/replay/resume", summary="Resume a paused replay.")
async def resume(request: Request) -> dict:
    return _engine(request).resume().as_dict()


@app.post("/replay/stop", summary="Stop the replay.")
async def stop(request: Request) -> dict:
    return (await _engine(request).stop()).as_dict()


@app.post("/replay/speed", summary="Change the speed multiplier mid-replay.")
async def speed(request: Request, body: SpeedRequest) -> dict:
    try:
        return _engine(request).set_speed(body.speed).as_dict()
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc


@app.post("/replay/seek", summary="Jump to a timestamp or a lap.")
async def seek(request: Request, body: SeekRequest) -> dict:
    engine = _engine(request)
    if engine.status.state is ReplayState.IDLE:
        raise HTTPException(status_code=409, detail="No replay is running.")

    target = body.to
    if body.lap is not None and engine.status.session_id is not None:
        target = await engine._time_of_lap(engine.status.session_id, body.lap)
        if target is None:
            raise HTTPException(
                status_code=404, detail=f"Lap {body.lap} has no recorded start time."
            )
    if target is None:
        raise HTTPException(status_code=422, detail="Provide either `to` or `lap`.")

    return engine.seek(target).as_dict()


@app.get("/replay/status", summary="Current replay status.")
async def status(request: Request) -> dict:
    return _engine(request).status.as_dict()


@app.get("/replay/timeline/{session_id}", summary="Markers for the replay timeline.")
async def timeline(request: Request, session_id: int) -> dict:
    """Lap starts and race-control events, for the scrubber.

    Only markers that exist in the data are returned. A session without race
    control simply has none, rather than being given synthetic ones.
    """
    database: Database = request.app.state.database

    laps = await database.fetch(
        """
        SELECT lap_number, min(lap_start) AS at
        FROM laps WHERE session_id = $1
        GROUP BY lap_number ORDER BY lap_number
        """,
        session_id,
    )
    control = await database.fetch(
        """
        SELECT event_time AS at, category, flag, message, lap_number
        FROM race_control_events WHERE session_id = $1
        ORDER BY event_time
        """,
        session_id,
    )

    return {
        "session_id": session_id,
        "laps": [{"lap_number": r["lap_number"], "at": r["at"]} for r in laps],
        "race_control": [
            {
                "at": r["at"],
                "category": r["category"],
                "flag": r["flag"],
                "message": r["message"],
                "lap_number": r["lap_number"],
            }
            for r in control
        ],
    }


@app.get("/health", summary="Service health.")
async def health(request: Request) -> JSONResponse:
    database: Database = request.app.state.database
    healthy, rtt, error = await database.healthcheck()
    body = {
        "state": "healthy" if healthy else "down",
        "database_latency_ms": round(rtt * 1000, 2) if rtt is not None else None,
        "detail": error,
        "replay": _engine(request).status.as_dict(),
    }
    return JSONResponse(body, status_code=200 if healthy else 503)


@app.get("/metrics", include_in_schema=False)
async def metrics() -> Response:
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)


def main() -> None:
    import uvicorn

    uvicorn.run(
        "racestream_replay.main:app",
        host="0.0.0.0",
        port=8001,
        log_level="warning",
    )


if __name__ == "__main__":
    main()
