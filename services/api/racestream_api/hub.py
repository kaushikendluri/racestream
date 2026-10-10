"""WebSocket connection hub and live-stream fanout.

Why the API consumes Kafka directly
-----------------------------------
The API joins its own consumer group, ``racestream-websocket``, reading the same
topics as the stream processor. It does not receive events *from* the processor.

That is the point of consumer groups: two independent readers of the same log,
each at its own offset, neither able to slow the other down. It means the live
view keeps working when the processor is restarted or scaled, and it means a
slow WebSocket client applies backpressure to the API's own consumer and nothing
else. Having the processor push to the API would have coupled persistence to
browser delivery, so that a stalled browser could stall the database writes.

The cost is that derivation happens twice. It is sub-millisecond per event
(measured, see docs/benchmarks.md), and both copies run the same
``racestream_common.domain`` code, so they cannot disagree.

Backpressure to the browser
---------------------------
A WebSocket client that stops reading must never block the fanout. Each
connection owns a bounded queue; when it fills, the *oldest* frames are dropped
rather than the newest, because stale telemetry has no value - a client that
fell behind wants the current state of the race, not a replay of what it missed.
Drops are counted, and the client is told how many it missed so the UI can be
honest about it.
"""

from __future__ import annotations

import asyncio
import contextlib
from dataclasses import dataclass, field
from typing import Any

import orjson
from fastapi import WebSocket
from starlette.websockets import WebSocketState

from racestream_common.domain import SessionState
from racestream_common.obs import METRICS, get_logger
from racestream_common.obs.metrics import utcnow
from racestream_common.schemas import Event, EventType

log = get_logger(__name__)

# Per-connection send queue. Sized to about two seconds of a live feed
# (~152 events/s, measured), so an ordinary network hiccup is absorbed while a
# genuinely stuck client is detected quickly.
CLIENT_QUEUE_SIZE = 256

# The browser cannot usefully repaint faster than this, and a timing tower that
# updates 150 times a second is unreadable. Events are coalesced into periodic
# snapshots instead of forwarded one by one.
BROADCAST_INTERVAL_S = 0.1


@dataclass
class Subscription:
    """What one client asked for.

    Defaults are deliberately narrow: a client gets session-level timing unless
    it opts into the high-volume channels. Sending every telemetry sample to a
    browser that only renders a timing tower wastes most of the bandwidth.
    """

    session_id: int | None = None
    driver_numbers: set[int] = field(default_factory=set)
    channels: set[str] = field(
        default_factory=lambda: {"timing", "session", "race_control"}
    )

    def wants(self, event: Event) -> bool:
        if self.session_id is not None and event.session_id != self.session_id:
            return False

        channel = _CHANNEL_OF.get(event.event_type)
        if channel is None or channel not in self.channels:
            return False

        # A driver filter only applies to per-driver events; session-scoped
        # events (weather, race control) are never filtered out by it.
        if self.driver_numbers and event.driver_id is not None:
            return event.driver_id in self.driver_numbers
        return True


_CHANNEL_OF: dict[EventType, str] = {
    EventType.CAR_TELEMETRY: "telemetry",
    EventType.POSITION: "position",
    EventType.TIMING: "timing",
    EventType.LAP: "timing",
    EventType.WEATHER: "session",
    EventType.SESSION_META: "session",
    EventType.RACE_CONTROL: "race_control",
}

VALID_CHANNELS = frozenset({"telemetry", "position", "timing", "session", "race_control"})


class Connection:
    """One browser. Owns its queue, its subscription and its own send loop."""

    _next_id = 0

    def __init__(self, websocket: WebSocket) -> None:
        Connection._next_id += 1
        self.id = Connection._next_id
        self.websocket = websocket
        self.subscription = Subscription()
        self.queue: asyncio.Queue[bytes] = asyncio.Queue(maxsize=CLIENT_QUEUE_SIZE)
        self.dropped = 0
        self.sent = 0
        self.connected_at = utcnow()
        self._sender: asyncio.Task | None = None

    async def start(self) -> None:
        self._sender = asyncio.create_task(self._send_loop(), name=f"ws-send-{self.id}")

    async def stop(self) -> None:
        if self._sender is not None:
            self._sender.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._sender
            self._sender = None

    def enqueue(self, frame: bytes) -> None:
        """Queue a frame, dropping the oldest if this client has fallen behind.

        Never blocks and never awaits: the fanout calls this for every
        connection in turn, and one slow client must not delay the others.
        """
        try:
            self.queue.put_nowait(frame)
        except asyncio.QueueFull:
            # Drop the oldest. A client that is behind wants the current state
            # of the race, not the frames it already missed.
            with contextlib.suppress(asyncio.QueueEmpty):
                self.queue.get_nowait()
                self.dropped += 1
                METRICS.messages_dropped.labels(
                    reason="slow_websocket_client", stage="fanout"
                ).inc()
            with contextlib.suppress(asyncio.QueueFull):
                self.queue.put_nowait(frame)

    async def _send_loop(self) -> None:
        try:
            while True:
                frame = await self.queue.get()
                if self.websocket.client_state is not WebSocketState.CONNECTED:
                    return
                await self.websocket.send_bytes(frame)
                self.sent += 1
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            # A disconnect mid-send is ordinary, not an error worth alarming on.
            log.debug("ws.send_loop_ended", connection=self.id, error=str(exc))

    def stats(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "connected_at": self.connected_at.isoformat(),
            "sent": self.sent,
            "dropped": self.dropped,
            "queued": self.queue.qsize(),
            "subscription": {
                "session_id": self.subscription.session_id,
                "channels": sorted(self.subscription.channels),
                "drivers": sorted(self.subscription.driver_numbers),
            },
        }


class LiveHub:
    """Holds the connections, the derived state, and the broadcast loop."""

    def __init__(self) -> None:
        self._connections: dict[int, Connection] = {}
        self._sessions: dict[int, SessionState] = {}
        self._lock = asyncio.Lock()
        self._broadcaster: asyncio.Task | None = None
        self._dirty: set[int] = set()
        self._stopping = False

        self.events_received = 0
        self.frames_broadcast = 0
        self.last_event_at: str | None = None

    # ---------------------------------------------------------------- lifecycle

    async def start(self) -> None:
        self._stopping = False
        self._broadcaster = asyncio.create_task(self._broadcast_loop(), name="ws-broadcast")

    async def stop(self) -> None:
        self._stopping = True
        if self._broadcaster is not None:
            self._broadcaster.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._broadcaster
            self._broadcaster = None
        async with self._lock:
            for connection in list(self._connections.values()):
                await connection.stop()
            self._connections.clear()
        METRICS.websocket_connections.set(0)

    # -------------------------------------------------------------- connections

    async def connect(self, websocket: WebSocket) -> Connection:
        connection = Connection(websocket)
        await connection.start()
        async with self._lock:
            self._connections[connection.id] = connection
        METRICS.websocket_connections.set(len(self._connections))
        log.info("ws.connected", connection=connection.id, total=len(self._connections))
        return connection

    async def disconnect(self, connection: Connection) -> None:
        await connection.stop()
        async with self._lock:
            self._connections.pop(connection.id, None)
        METRICS.websocket_connections.set(len(self._connections))
        log.info(
            "ws.disconnected",
            connection=connection.id,
            sent=connection.sent,
            dropped=connection.dropped,
            total=len(self._connections),
        )

    @property
    def connection_count(self) -> int:
        return len(self._connections)

    # ------------------------------------------------------------------ ingest

    async def on_events(self, events: list[Event]) -> None:
        """Apply a consumed batch to the derived state.

        Nothing is sent here. Frames go out on the broadcast tick, which
        coalesces a burst of events into one snapshot per session - the browser
        cannot repaint 150 times a second, and a tower that tried to would be
        unreadable.
        """
        for event in events:
            state = self._sessions.get(event.session_id)
            if state is None:
                state = SessionState(session_id=event.session_id)
                self._sessions[event.session_id] = state
            state.apply(event)
            self._dirty.add(event.session_id)

        self.events_received += len(events)
        if events:
            self.last_event_at = utcnow().isoformat()

        # Race control is the exception: a red flag must not wait for the next
        # tick, and it is rare enough that immediate delivery costs nothing.
        urgent = [e for e in events if e.event_type is EventType.RACE_CONTROL]
        if urgent:
            await self._broadcast_events(urgent)

    # --------------------------------------------------------------- broadcast

    async def _broadcast_loop(self) -> None:
        while not self._stopping:
            try:
                await asyncio.sleep(BROADCAST_INTERVAL_S)
                if not self._dirty or not self._connections:
                    self._dirty.clear()
                    continue
                dirty, self._dirty = self._dirty, set()
                for session_id in dirty:
                    await self._broadcast_snapshot(session_id)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                METRICS.errors.labels(component="ws_hub", kind=type(exc).__name__).inc()
                log.error("ws.broadcast_error", error=str(exc))

    async def _broadcast_snapshot(self, session_id: int) -> None:
        state = self._sessions.get(session_id)
        if state is None:
            return

        payload = {
            "type": "snapshot",
            "sent_at": utcnow().isoformat(),
            "state": state.as_dict(),
        }
        frame = orjson.dumps(payload)

        for connection in list(self._connections.values()):
            sub = connection.subscription
            if sub.session_id is not None and sub.session_id != session_id:
                continue
            connection.enqueue(frame)

        self.frames_broadcast += 1

    async def _broadcast_events(self, events: list[Event]) -> None:
        """Send individual events to the clients that subscribed to them."""
        for event in events:
            stamped = event.model_copy(update={"trace": event.trace.stamp("ws_ts")})
            frame = orjson.dumps(
                {
                    "type": "event",
                    "sent_at": utcnow().isoformat(),
                    "event": stamped.model_dump(mode="json"),
                }
            )
            for connection in list(self._connections.values()):
                if connection.subscription.wants(stamped):
                    connection.enqueue(frame)

    # ----------------------------------------------------------------- queries

    def snapshot(self, session_id: int) -> dict:
        state = self._sessions.get(session_id)
        if state is None:
            return {"session_id": session_id, "drivers": []}
        return state.as_dict()

    def active_sessions(self) -> list[int]:
        return sorted(self._sessions)

    def stats(self) -> dict:
        return {
            "connections": self.connection_count,
            "events_received": self.events_received,
            "frames_broadcast": self.frames_broadcast,
            "last_event_at": self.last_event_at,
            "active_sessions": self.active_sessions(),
            "clients": [c.stats() for c in self._connections.values()],
        }
