"""Replay control, proxied to the replay service.

The frontend talks to one origin. This router forwards to the replay service
rather than reimplementing its logic, so there is exactly one replay state
machine in the system and no possibility of the two disagreeing about whether a
replay is running.

The proxy is deliberately thin: it forwards the body, returns the upstream
status code, and translates a connection failure into a 503 that names the
service. It does not interpret replay semantics.
"""

from __future__ import annotations

from typing import Any

import httpx
from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import JSONResponse

from racestream_api.config import get_api_settings
from racestream_common.obs import get_logger

router = APIRouter(prefix="/api/replay", tags=["replay"])
log = get_logger(__name__)

# Replay operations are quick to acknowledge: start returns as soon as the
# background task is scheduled, not when the replay finishes.
TIMEOUT_S = 10.0


async def _forward(
    method: str, path: str, json_body: dict[str, Any] | None = None
) -> JSONResponse:
    base = get_api_settings().replay_service_url.rstrip("/")
    url = f"{base}{path}"

    try:
        async with httpx.AsyncClient(timeout=TIMEOUT_S) as client:
            response = await client.request(method, url, json=json_body)
    except httpx.TimeoutException as exc:
        raise HTTPException(
            status_code=504,
            detail=f"The replay service did not respond within {TIMEOUT_S:.0f}s.",
        ) from exc
    except httpx.TransportError as exc:
        # Name the dependency. "Internal error" would send someone looking in
        # the wrong service.
        raise HTTPException(
            status_code=503,
            detail=f"The replay service is unreachable at {base}.",
        ) from exc

    try:
        payload = response.json()
    except ValueError:
        payload = {"detail": response.text[:500]}

    # Pass the upstream status through unchanged: a 409 "already running" must
    # not be flattened into a 500.
    return JSONResponse(payload, status_code=response.status_code)


@router.post("/start", summary="Start replaying a session through the pipeline.")
async def start(request: Request) -> JSONResponse:
    return await _forward("POST", "/replay/start", await _body(request))


@router.post("/pause", summary="Pause the running replay.")
async def pause() -> JSONResponse:
    return await _forward("POST", "/replay/pause")


@router.post("/resume", summary="Resume the paused replay.")
async def resume() -> JSONResponse:
    return await _forward("POST", "/replay/resume")


@router.post("/stop", summary="Stop the replay.")
async def stop() -> JSONResponse:
    return await _forward("POST", "/replay/stop")


@router.post("/speed", summary="Change the replay speed multiplier.")
async def speed(request: Request) -> JSONResponse:
    return await _forward("POST", "/replay/speed", await _body(request))


@router.post("/seek", summary="Jump to a timestamp or lap.")
async def seek(request: Request) -> JSONResponse:
    return await _forward("POST", "/replay/seek", await _body(request))


@router.get("/status", summary="Current replay status.")
async def status() -> JSONResponse:
    return await _forward("GET", "/replay/status")


@router.get("/timeline/{session_id}", summary="Lap and race-control markers.")
async def timeline(session_id: int) -> JSONResponse:
    return await _forward("GET", f"/replay/timeline/{session_id}")


async def _body(request: Request) -> dict[str, Any]:
    try:
        payload = await request.json()
    except Exception:
        return {}
    return payload if isinstance(payload, dict) else {}
