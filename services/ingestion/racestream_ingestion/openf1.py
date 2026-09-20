"""OpenF1 HTTP client.

The single most important constraint on this service is measured, not assumed:
**OpenF1 enforces a hard limit of 3 requests per second** and answers a fourth
with HTTP 429. Everything here is shaped by that.

    $ curl .../stints?session_key=9472
    {"detail":"Rate limit exceeded. Max 3 requests/second.", ...}

A concurrency cap alone does not satisfy a *rate* limit - three concurrent
requests that each take 50ms is 60 requests per second. So this client holds a
token bucket as well, and every request waits for a token before it is sent.
"""

from __future__ import annotations

import asyncio
import time
from datetime import datetime
from types import TracebackType
from typing import Any, Sequence

import httpx

from racestream_common.config import OpenF1Settings
from racestream_common.obs import METRICS, get_logger

log = get_logger(__name__)


class OpenF1Error(RuntimeError):
    """A request failed after exhausting retries."""

    def __init__(self, endpoint: str, message: str, status: int | None = None) -> None:
        super().__init__(f"{endpoint}: {message}")
        self.endpoint = endpoint
        self.status = status


class RateLimiter:
    """Token bucket.

    Capacity equals the burst we are willing to spend at once; tokens refill at
    ``rate`` per second. Sized slightly under the published limit so that clock
    skew between us and the server does not put us over it.
    """

    def __init__(self, rate_per_second: float, capacity: int | None = None) -> None:
        self._rate = rate_per_second
        self._capacity = capacity if capacity is not None else max(1, int(rate_per_second))
        self._tokens = float(self._capacity)
        self._updated = time.monotonic()
        self._lock = asyncio.Lock()

    async def acquire(self) -> None:
        # The lock serialises the check-and-decrement. Without it, concurrent
        # callers could each observe the same token and both proceed.
        async with self._lock:
            while True:
                now = time.monotonic()
                self._tokens = min(
                    self._capacity, self._tokens + (now - self._updated) * self._rate
                )
                self._updated = now
                if self._tokens >= 1.0:
                    self._tokens -= 1.0
                    return
                await asyncio.sleep((1.0 - self._tokens) / self._rate)


class OpenF1Client:
    """Async client for the public OpenF1 API.

    Responsibilities kept here so the ingestion logic never deals with them:
    rate limiting, retry with backoff, timeouts, and latency metrics.
    """

    # Retried: transient upstream conditions. 429 is retried because the limit
    # is per-second, so waiting genuinely resolves it.
    RETRYABLE_STATUS = frozenset({429, 500, 502, 503, 504})

    def __init__(self, settings: OpenF1Settings, rate_per_second: float = 2.5) -> None:
        self._settings = settings
        self._base_url = settings.base_url.rstrip("/")
        self._limiter = RateLimiter(rate_per_second)
        self._semaphore = asyncio.Semaphore(settings.max_concurrent_requests)
        self._client: httpx.AsyncClient | None = None

    async def start(self) -> None:
        self._client = httpx.AsyncClient(
            timeout=httpx.Timeout(self._settings.timeout_s, connect=10.0),
            # Telemetry windows are megabytes; connection reuse matters.
            limits=httpx.Limits(
                max_connections=self._settings.max_concurrent_requests,
                max_keepalive_connections=self._settings.max_concurrent_requests,
            ),
            headers={"Accept": "application/json", "User-Agent": "RaceStream/0.1 (+github.com/kaushikendluri/racestream)"},
            follow_redirects=True,
        )
        log.info("openf1.client_started", base_url=self._base_url)

    async def close(self) -> None:
        if self._client is not None:
            await self._client.aclose()
            self._client = None

    async def __aenter__(self) -> "OpenF1Client":
        await self.start()
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        await self.close()

    async def get(self, endpoint: str, params: dict[str, Any]) -> list[dict[str, Any]]:
        """Fetch one endpoint, retrying transient failures.

        Returns a list of raw upstream records. Validation happens in the
        mapper, not here: this layer's job is to get bytes reliably.
        """
        if self._client is None:
            raise RuntimeError("client not started")

        url = f"{self._base_url}/{endpoint.lstrip('/')}"
        clean = {k: v for k, v in params.items() if v is not None}
        last_error: str = "unknown"
        last_status: int | None = None

        for attempt in range(1, self._settings.max_retries + 2):
            await self._limiter.acquire()
            started = time.perf_counter()
            try:
                async with self._semaphore:
                    response = await self._client.get(url, params=clean)
                elapsed = time.perf_counter() - started

                if response.status_code == 200:
                    METRICS.upstream_request_latency.labels(
                        endpoint=endpoint, outcome="success"
                    ).observe(elapsed)
                    payload = response.json()
                    # A single object where a list is expected is a contract
                    # change we want to see, not silently coerce.
                    if not isinstance(payload, list):
                        raise OpenF1Error(
                            endpoint, f"expected a JSON array, got {type(payload).__name__}"
                        )
                    return payload

                last_status = response.status_code
                last_error = response.text[:200]
                METRICS.upstream_request_latency.labels(
                    endpoint=endpoint, outcome=f"http_{response.status_code}"
                ).observe(elapsed)

                if response.status_code not in self.RETRYABLE_STATUS:
                    raise OpenF1Error(endpoint, last_error, response.status_code)

                # Honour Retry-After when the server sends one; it knows better
                # than our backoff curve does.
                retry_after = response.headers.get("Retry-After")
                delay = (
                    float(retry_after)
                    if retry_after and retry_after.replace(".", "", 1).isdigit()
                    else self._settings.backoff_base_s * (2 ** (attempt - 1))
                )
                log.warning(
                    "openf1.retrying",
                    endpoint=endpoint,
                    status=response.status_code,
                    attempt=attempt,
                    delay_s=round(delay, 2),
                )
                METRICS.errors.labels(
                    component="openf1", kind=f"http_{response.status_code}"
                ).inc()
                await asyncio.sleep(delay)

            except (httpx.TimeoutException, httpx.TransportError) as exc:
                last_error = f"{type(exc).__name__}: {exc}"
                METRICS.upstream_request_latency.labels(
                    endpoint=endpoint, outcome="transport_error"
                ).observe(time.perf_counter() - started)
                METRICS.errors.labels(component="openf1", kind=type(exc).__name__).inc()
                delay = self._settings.backoff_base_s * (2 ** (attempt - 1))
                log.warning(
                    "openf1.transport_retry",
                    endpoint=endpoint,
                    attempt=attempt,
                    error=last_error,
                    delay_s=round(delay, 2),
                )
                await asyncio.sleep(delay)

        raise OpenF1Error(
            endpoint,
            f"failed after {self._settings.max_retries + 1} attempts: {last_error}",
            last_status,
        )

    # ------------------------------------------------------------- endpoints
    #
    # Thin, named wrappers. They exist so that a typo in an endpoint name is a
    # missing attribute rather than an empty result set at runtime.

    async def sessions(
        self,
        year: int | None = None,
        session_key: int | None = None,
        session_name: str | None = None,
        country_name: str | None = None,
    ) -> list[dict[str, Any]]:
        return await self.get(
            "sessions",
            {
                "year": year,
                "session_key": session_key,
                "session_name": session_name,
                "country_name": country_name,
            },
        )

    async def drivers(self, session_key: int) -> list[dict[str, Any]]:
        return await self.get("drivers", {"session_key": session_key})

    async def laps(
        self, session_key: int, driver_number: int | None = None
    ) -> list[dict[str, Any]]:
        return await self.get(
            "laps", {"session_key": session_key, "driver_number": driver_number}
        )

    async def stints(self, session_key: int) -> list[dict[str, Any]]:
        return await self.get("stints", {"session_key": session_key})

    async def weather(self, session_key: int) -> list[dict[str, Any]]:
        return await self.get("weather", {"session_key": session_key})

    async def race_control(self, session_key: int) -> list[dict[str, Any]]:
        return await self.get("race_control", {"session_key": session_key})

    async def intervals(self, session_key: int) -> list[dict[str, Any]]:
        return await self.get("intervals", {"session_key": session_key})

    async def positions(self, session_key: int) -> list[dict[str, Any]]:
        """Classification positions. Distinct from ``location`` (track x/y/z)."""
        return await self.get("position", {"session_key": session_key})

    async def car_data(
        self,
        session_key: int,
        start: datetime,
        end: datetime,
        driver_number: int | None = None,
    ) -> list[dict[str, Any]]:
        """Car telemetry in a time window.

        Windowed rather than fetched whole: a two-hour race is over half a
        million rows across all drivers, which is neither a sensible single
        response nor something to hold in memory at once.
        """
        return await self.get(
            "car_data",
            {
                "session_key": session_key,
                "driver_number": driver_number,
                "date>": _iso(start),
                "date<": _iso(end),
            },
        )

    async def location(
        self,
        session_key: int,
        start: datetime,
        end: datetime,
        driver_number: int | None = None,
    ) -> list[dict[str, Any]]:
        """Track position (x, y, z) in a time window."""
        return await self.get(
            "location",
            {
                "session_key": session_key,
                "driver_number": driver_number,
                "date>": _iso(start),
                "date<": _iso(end),
            },
        )


def _iso(value: datetime) -> str:
    """Format a datetime the way OpenF1's date filters expect.

    The API wants a naive-looking ISO string; sending an offset makes the
    comparison unreliable, so the value is converted to UTC and the offset is
    dropped rather than being reinterpreted.
    """
    from datetime import timezone

    if value.tzinfo is not None:
        value = value.astimezone(timezone.utc).replace(tzinfo=None)
    return value.isoformat(timespec="milliseconds")


def chunk_windows(
    start: datetime, end: datetime, window_seconds: int
) -> Sequence[tuple[datetime, datetime]]:
    """Split a session into fetch windows.

    300s measured at ~22k rows and ~3.7MB per request across all drivers, which
    is a good balance: large enough that a race is a few dozen requests, small
    enough that one failure re-fetches little.
    """
    from datetime import timedelta

    if end <= start:
        return []
    step = timedelta(seconds=window_seconds)
    windows: list[tuple[datetime, datetime]] = []
    cursor = start
    while cursor < end:
        nxt = min(cursor + step, end)
        windows.append((cursor, nxt))
        cursor = nxt
    return windows
