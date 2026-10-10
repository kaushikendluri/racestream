"""WebSocket endpoint and live-state REST views.

Protocol (client -> server), one JSON object per message:

    {"type": "subscribe",   "session_id": 9472,
                            "channels": ["timing", "telemetry"],
                            "drivers": [1, 16]}
    {"type": "unsubscribe", "channels": ["telemetry"]}
    {"type": "ping"}

Server -> client:

    {"type": "hello",    ...}   once, on connect
    {"type": "snapshot", ...}   coalesced session state, ~10/s
    {"type": "event",    ...}   individual event (race control)
    {"type": "pong",     ...}   reply to ping
    {"type": "error",    ...}   a message we could not act on

Every inbound message is validated. A malformed one gets an error reply and the
connection stays open: a client bug should not cost the user their live feed.
"""

from __future__ import annotations

import asyncio
import contextlib

import orjson
from fastapi import APIRouter, HTTPException, Query, Request, WebSocket, WebSocketDisconnect

from racestream_api.hub import VALID_CHANNELS, Connection, LiveHub
from racestream_common.obs import METRICS, get_logger
from racestream_common.obs.metrics import utcnow

router = APIRouter(tags=["live"])
log = get_logger(__name__)

# If a client sends nothing at all for this long - not even a ping - it is
# assumed gone. Browsers that are backgrounded stop timers, so this is generous.
CLIENT_IDLE_TIMEOUT_S = 120.0

MAX_MESSAGE_BYTES = 4096
MAX_DRIVER_FILTER = 30


def _hub(request_or_ws) -> LiveHub:
    return request_or_ws.app.state.racestream.hub


@router.websocket("/ws/live")
async def live_stream(websocket: WebSocket) -> None:
    hub = _hub(websocket)
    await websocket.accept()
    connection = await hub.connect(websocket)

    try:
        await websocket.send_bytes(
            orjson.dumps(
                {
                    "type": "hello",
                    "connection_id": connection.id,
                    "server_time": utcnow().isoformat(),
                    "active_sessions": hub.active_sessions(),
                    "channels": sorted(VALID_CHANNELS),
                    # The client mirrors these so both ends agree on when a
                    # stream stops being "live".
                    "stale_after_s": 5.0,
                    "disconnected_after_s": 15.0,
                }
            )
        )

        while True:
            try:
                raw = await asyncio.wait_for(
                    websocket.receive_text(), timeout=CLIENT_IDLE_TIMEOUT_S
                )
            except asyncio.TimeoutError:
                log.info("ws.idle_timeout", connection=connection.id)
                break

            if len(raw) > MAX_MESSAGE_BYTES:
                await _error(websocket, "message_too_large", "Message exceeds 4096 bytes.")
                continue

            await _handle_message(websocket, connection, hub, raw)

    except WebSocketDisconnect:
        pass  # ordinary: the user closed the tab
    except Exception as exc:
        METRICS.errors.labels(component="websocket", kind=type(exc).__name__).inc()
        log.warning("ws.connection_error", connection=connection.id, error=str(exc))
    finally:
        await hub.disconnect(connection)
        with contextlib.suppress(Exception):
            await websocket.close()


async def _handle_message(
    websocket: WebSocket, connection: Connection, hub: LiveHub, raw: str
) -> None:
    try:
        message = orjson.loads(raw)
    except orjson.JSONDecodeError:
        await _error(websocket, "invalid_json", "Message was not valid JSON.")
        return

    if not isinstance(message, dict):
        await _error(websocket, "invalid_message", "Message must be a JSON object.")
        return

    kind = message.get("type")

    if kind == "ping":
        # The heartbeat carries the server clock so the client can detect drift
        # and decide whether its own staleness timer is trustworthy.
        await websocket.send_bytes(
            orjson.dumps({"type": "pong", "server_time": utcnow().isoformat()})
        )
        return

    if kind == "subscribe":
        session_id = message.get("session_id")
        if session_id is not None:
            if not isinstance(session_id, int) or session_id <= 0:
                await _error(websocket, "invalid_session", "session_id must be a positive integer.")
                return
            connection.subscription.session_id = session_id

        channels = message.get("channels")
        if channels is not None:
            if not isinstance(channels, list):
                await _error(websocket, "invalid_channels", "channels must be a list.")
                return
            unknown = set(channels) - VALID_CHANNELS
            if unknown:
                await _error(
                    websocket,
                    "unknown_channel",
                    f"Unknown channel(s): {sorted(unknown)}. Valid: {sorted(VALID_CHANNELS)}",
                )
                return
            connection.subscription.channels = set(channels)

        drivers = message.get("drivers")
        if drivers is not None:
            if not isinstance(drivers, list) or len(drivers) > MAX_DRIVER_FILTER:
                await _error(
                    websocket,
                    "invalid_drivers",
                    f"drivers must be a list of at most {MAX_DRIVER_FILTER} driver numbers.",
                )
                return
            if not all(isinstance(d, int) for d in drivers):
                await _error(websocket, "invalid_drivers", "driver numbers must be integers.")
                return
            connection.subscription.driver_numbers = set(drivers)

        await websocket.send_bytes(
            orjson.dumps({"type": "subscribed", **connection.stats()["subscription"]})
        )

        # Send the current state immediately rather than making the client wait
        # for the next tick: a freshly-opened tower should not be blank.
        if connection.subscription.session_id is not None:
            await websocket.send_bytes(
                orjson.dumps(
                    {
                        "type": "snapshot",
                        "sent_at": utcnow().isoformat(),
                        "state": hub.snapshot(connection.subscription.session_id),
                    }
                )
            )
        return

    if kind == "unsubscribe":
        channels = message.get("channels")
        if isinstance(channels, list):
            connection.subscription.channels -= set(channels)
        else:
            connection.subscription.channels.clear()
        await websocket.send_bytes(
            orjson.dumps({"type": "subscribed", **connection.stats()["subscription"]})
        )
        return

    await _error(websocket, "unknown_type", f"Unknown message type: {kind!r}")


async def _error(websocket: WebSocket, code: str, detail: str) -> None:
    """Report a client error without closing the connection."""
    with contextlib.suppress(Exception):
        await websocket.send_bytes(
            orjson.dumps({"type": "error", "error": code, "detail": detail})
        )


# ------------------------------------------------------------------ REST views


@router.get(
    "/api/live/sessions",
    summary="Sessions currently producing events on the stream.",
)
async def live_sessions(request: Request) -> dict:
    hub = _hub(request)
    return {
        "active_sessions": hub.active_sessions(),
        "events_received": hub.events_received,
        "last_event_at": hub.last_event_at,
    }


@router.get(
    "/api/live/{session_id}",
    summary="Current derived state for a session, as the WebSocket would send it.",
)
async def live_snapshot(request: Request, session_id: int) -> dict:
    """The same payload the WebSocket pushes.

    Exists so the frontend can render immediately on load, and so the live view
    is inspectable with curl without opening a socket.
    """
    hub = _hub(request)
    if session_id not in hub.active_sessions():
        raise HTTPException(
            status_code=404,
            detail=f"No live state for session {session_id}. "
            "Nothing has streamed for it since this API started.",
        )
    return {"sent_at": utcnow().isoformat(), "state": hub.snapshot(session_id)}


@router.get(
    "/api/system/websocket",
    summary="WebSocket fanout statistics.",
)
async def websocket_stats(
    request: Request,
    include_clients: bool = Query(
        default=False, description="Include per-connection detail."
    ),
) -> dict:
    stats = _hub(request).stats()
    if not include_clients:
        stats.pop("clients", None)
    return stats
