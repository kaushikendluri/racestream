"""Health, readiness and component status.

These endpoints back the System Health screen, so they report what was actually
measured. A component whose latency could not be taken reports ``None``, and the
UI shows N/A rather than a number nobody measured.
"""

from __future__ import annotations

import asyncio
from datetime import datetime, timezone

from fastapi import APIRouter, Request, Response, status

from racestream_api.models import ComponentHealth, HealthResponse, HealthState, ReadinessResponse
from racestream_api.state import AppState
from racestream_common.config import get_settings
from racestream_common.obs import METRICS, get_logger

router = APIRouter(tags=["system"])
log = get_logger(__name__)

API_VERSION = "0.1.0"


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _worst(states: list[HealthState]) -> HealthState:
    """Aggregate: the system is only as healthy as its least healthy component."""
    order = [HealthState.DOWN, HealthState.DEGRADED, HealthState.UNKNOWN, HealthState.HEALTHY]
    for candidate in order:
        if candidate in states:
            return candidate
    return HealthState.UNKNOWN


async def _check_database(state: AppState) -> ComponentHealth:
    if state.database is None or not state.database.connected:
        return ComponentHealth(
            name="database",
            state=HealthState.DOWN,
            detail="Connection pool is not open.",
            latency_ms=None,
            checked_at=_now(),
        )
    healthy, rtt_seconds, error = await state.database.healthcheck()
    if not healthy:
        return ComponentHealth(
            name="database",
            state=HealthState.DOWN,
            detail=error,
            latency_ms=None,
            checked_at=_now(),
        )
    latency_ms = rtt_seconds * 1000 if rtt_seconds is not None else None
    # A reachable but slow database is degraded, not healthy - saying otherwise
    # would hide the exact condition this screen exists to surface.
    degraded = latency_ms is not None and latency_ms > 250
    return ComponentHealth(
        name="database",
        state=HealthState.DEGRADED if degraded else HealthState.HEALTHY,
        detail="Round-trip above 250ms." if degraded else None,
        latency_ms=round(latency_ms, 2) if latency_ms is not None else None,
        checked_at=_now(),
    )


async def _check_kafka() -> ComponentHealth:
    """Probe the broker with a short-lived metadata request.

    Deliberately a fresh client with a tight timeout: the point is to answer
    "can the broker be reached right now", not to report on a cached connection.
    """
    settings = get_settings().kafka
    checked_at = _now()
    try:
        from aiokafka.admin import AIOKafkaAdminClient

        admin = AIOKafkaAdminClient(
            bootstrap_servers=settings.bootstrap_servers,
            client_id=f"{settings.client_id}-healthcheck",
            request_timeout_ms=3000,
        )
        loop = asyncio.get_running_loop()
        started = loop.time()
        await asyncio.wait_for(admin.start(), timeout=4.0)
        try:
            topics = await asyncio.wait_for(admin.list_topics(), timeout=4.0)
        finally:
            await admin.close()
        latency_ms = (loop.time() - started) * 1000
        METRICS.service_up.labels(component="streaming").set(1)
        return ComponentHealth(
            name="streaming",
            state=HealthState.HEALTHY,
            detail=f"{len(topics)} topics visible.",
            latency_ms=round(latency_ms, 2),
            checked_at=checked_at,
        )
    except asyncio.TimeoutError:
        METRICS.service_up.labels(component="streaming").set(0)
        return ComponentHealth(
            name="streaming",
            state=HealthState.DOWN,
            detail="Broker metadata request timed out.",
            latency_ms=None,
            checked_at=checked_at,
        )
    except Exception as exc:
        METRICS.service_up.labels(component="streaming").set(0)
        return ComponentHealth(
            name="streaming",
            state=HealthState.DOWN,
            detail=str(exc)[:200],
            latency_ms=None,
            checked_at=checked_at,
        )


@router.get(
    "/api/system/health",
    response_model=HealthResponse,
    summary="Aggregate health of every component the API depends on.",
)
async def system_health(request: Request, response: Response) -> HealthResponse:
    state: AppState = request.app.state.racestream

    # Probe concurrently: a slow component should not delay the whole report.
    database, streaming = await asyncio.gather(
        _check_database(state), _check_kafka()
    )

    api_component = ComponentHealth(
        name="api",
        state=HealthState.HEALTHY,
        detail=None,
        latency_ms=None,  # measuring our own latency from inside is meaningless
        checked_at=_now(),
    )
    components = [api_component, database, streaming]
    overall = _worst([c.state for c in components])

    # 503 when not healthy, so that a load balancer or `curl --fail` sees it
    # without having to parse the body.
    if overall in (HealthState.DOWN, HealthState.UNKNOWN):
        response.status_code = status.HTTP_503_SERVICE_UNAVAILABLE

    return HealthResponse(
        state=overall,
        version=API_VERSION,
        environment=get_settings().obs.environment,
        uptime_seconds=round(state.uptime_seconds, 2),
        components=components,
    )


@router.get(
    "/api/system/live",
    summary="Liveness probe. Answers only whether the process is running.",
)
async def liveness() -> dict:
    return {"alive": True}


@router.get(
    "/api/system/ready",
    response_model=ReadinessResponse,
    summary="Readiness probe. Answers whether this instance can serve traffic.",
)
async def readiness(request: Request, response: Response) -> ReadinessResponse:
    state: AppState = request.app.state.racestream
    db_ok = state.database is not None and state.database.connected
    if db_ok:
        db_ok, _, _ = await state.database.healthcheck()

    checks = {"database": db_ok}
    ready = all(checks.values())
    if not ready:
        response.status_code = status.HTTP_503_SERVICE_UNAVAILABLE
    return ReadinessResponse(
        ready=ready,
        checks=checks,
        detail=None if ready else "Dependencies are not all available.",
    )
