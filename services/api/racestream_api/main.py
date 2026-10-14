"""RaceStream API.

Serves the REST surface, the WebSocket fanout and the service's own Prometheus
metrics. Startup and shutdown are managed through a lifespan context so that
the database pool and Kafka clients are opened once and closed cleanly.
"""

from __future__ import annotations

import time
from contextlib import asynccontextmanager
from typing import AsyncIterator

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, Response
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest

from racestream_api.config import get_api_settings, get_shared_settings
from racestream_api.hub import LiveHub
from racestream_api.models import ErrorResponse
from racestream_api.routers import health, live, replay, sessions
from racestream_api.state import AppState
from racestream_common.db import Database
from racestream_common.kafka import EventConsumer, EventProducer, ensure_topics
from racestream_common.obs import configure_logging, get_logger
from racestream_common.topics import POSITION, SESSION, TELEMETRY, TIMING

API_VERSION = "0.1.0"

# The API is a third consumer group alongside the processor's two.
WEBSOCKET_GROUP = "racestream-websocket"

settings = get_api_settings()
shared = get_shared_settings()

configure_logging(
    service="api",
    level=shared.obs.log_level,
    json_output=shared.obs.log_json,
    environment=shared.obs.environment,
)
log = get_logger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    """Own the lifecycle of every long-lived resource the API holds."""
    state: AppState = app.state.racestream
    state.started_at = time.monotonic()

    state.database = Database(shared.db)
    try:
        await state.database.connect()
    except Exception as exc:
        # A database that is not up yet must not prevent the API from starting:
        # /health has to be reachable precisely so it can report the problem.
        log.error("api.database_unavailable_at_startup", error=str(exc))

    # The live hub reads Kafka itself, in its own consumer group, rather than
    # being fed by the processor. Two independent readers of the same log at
    # their own offsets: the live view survives a processor restart, and a
    # stalled browser cannot stall the database writes. See hub.py.
    state.hub = LiveHub()
    await state.hub.start()

    try:
        await ensure_topics(shared.kafka.bootstrap_servers)
        state.dlq_producer = EventProducer(shared.kafka, "api-dlq")
        await state.dlq_producer.start()
        state.consumer = EventConsumer(
            settings=shared.kafka,
            topics=[TELEMETRY.name, POSITION.name, TIMING.name, SESSION.name],
            group_id=WEBSOCKET_GROUP,
            handler=state.hub.on_events,
            dlq_producer=state.dlq_producer,
            # `latest`, not `earliest`. The hub is a live view: its state is an
            # in-memory projection of the current stream, and replaying hours
            # of retained history to rebuild it would delay the live feed by
            # minutes for no benefit. Historical state is a database query,
            # served by the REST endpoints. The processor, whose job is to miss
            # nothing, uses `earliest`.
            auto_offset_reset="latest",
        )
        await state.consumer.start()
    except Exception as exc:
        # A broker that is not up yet must not stop the API from serving, for
        # the same reason the database must not: /health has to be reachable
        # in order to report the problem.
        log.error("api.stream_consumer_unavailable", error=str(exc))

    log.info(
        "api.started",
        version=API_VERSION,
        environment=shared.obs.environment,
        cors_origins=settings.cors_origin_list,
    )
    try:
        yield
    finally:
        if state.consumer is not None:
            await state.consumer.stop()
        if state.dlq_producer is not None:
            await state.dlq_producer.stop()
        if state.hub is not None:
            await state.hub.stop()
        if state.database is not None:
            await state.database.close()
        log.info("api.stopped")


app = FastAPI(
    title="RaceStream API",
    version=API_VERSION,
    summary="Real-time F1 telemetry, replay control and pipeline observability.",
    description=(
        "Serves session metadata, telemetry queries, live WebSocket updates and "
        "pipeline health for RaceStream.\n\n"
        "**Data source:** the public [OpenF1](https://openf1.org) API. This service "
        "is not affiliated with Formula 1 and exposes no proprietary team data.\n\n"
        "**On missing values:** fields that could not be measured are returned as "
        "`null` rather than substituted with a default."
    ),
    lifespan=lifespan,
    docs_url="/docs",
    redoc_url="/redoc",
    openapi_url="/openapi.json",
)
app.state.racestream = AppState()

app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.cors_origin_list,
    allow_credentials=True,
    allow_methods=["GET", "POST", "OPTIONS"],
    allow_headers=["Content-Type", "Authorization"],
)


@app.middleware("http")
async def access_log(request: Request, call_next):
    """One structured line per request, with a measured duration."""
    started = time.perf_counter()
    response = await call_next(request)
    elapsed_ms = (time.perf_counter() - started) * 1000
    # Use the route template, not the raw path: /sessions/{id} rather than
    # /sessions/9158, so the log stays aggregatable.
    route = request.scope.get("route")
    log.info(
        "http.request",
        method=request.method,
        path=getattr(route, "path", request.url.path),
        status=response.status_code,
        duration_ms=round(elapsed_ms, 2),
    )
    response.headers["X-Response-Time-Ms"] = f"{elapsed_ms:.2f}"
    return response


@app.exception_handler(Exception)
async def unhandled_exception_handler(request: Request, exc: Exception) -> JSONResponse:
    """Log the detail, return a generic body.

    Stack traces and driver messages belong in the logs, not in an HTTP response.
    """
    log.error(
        "http.unhandled_error",
        path=request.url.path,
        error=str(exc),
        kind=type(exc).__name__,
        exc_info=True,
    )
    return JSONResponse(
        status_code=500,
        content=ErrorResponse(
            error="internal_error",
            detail="The request could not be completed.",
            status_code=500,
            path=request.url.path,
        ).model_dump(),
    )


app.include_router(health.router)
app.include_router(live.router)
app.include_router(sessions.router)
app.include_router(replay.router)


if settings.expose_metrics_endpoint:

    @app.get(
        "/metrics",
        include_in_schema=False,
        summary="Prometheus exposition format.",
    )
    async def metrics() -> Response:
        return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)


@app.get("/", include_in_schema=False)
async def root() -> dict:
    return {
        "service": "racestream-api",
        "version": API_VERSION,
        "docs": "/docs",
        "health": "/api/system/health",
    }
