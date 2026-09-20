"""asyncpg connection pool.

asyncpg rather than SQLAlchemy: this service writes batches of a few hundred
rows at a time on a hot path and reads them back with hand-written time-series
queries. An ORM would add a mapping layer over work that is already expressed
most clearly as SQL, and asyncpg's binary protocol and ``executemany`` are a
material part of the write throughput. See docs/engineering-decisions.md.

Every query in this codebase is parameterised. No SQL is ever built by string
interpolation of user input.
"""

from __future__ import annotations

import asyncio
from types import TracebackType
from typing import Any, Sequence

import asyncpg

from racestream_common.config import DatabaseSettings
from racestream_common.obs import METRICS, get_logger

log = get_logger(__name__)


class Database:
    """Owns the pool and exposes the few access patterns services need."""

    def __init__(self, settings: DatabaseSettings) -> None:
        self._settings = settings
        self._pool: asyncpg.Pool | None = None

    async def connect(self, retries: int = 10, backoff_s: float = 1.5) -> None:
        """Open the pool, retrying while the database is still starting.

        Compose healthchecks cover most of this, but a database can also be
        restarted underneath a running service, so the retry lives here too.
        """
        last: Exception | None = None
        for attempt in range(1, retries + 1):
            try:
                self._pool = await asyncpg.create_pool(
                    dsn=self._settings.asyncpg_dsn,
                    min_size=self._settings.pool_min_size,
                    max_size=self._settings.pool_max_size,
                    command_timeout=self._settings.command_timeout,
                    # Server-side statement cache interacts badly with
                    # connection poolers; disabled so the code behaves the same
                    # whether or not one is in front of the database later.
                    statement_cache_size=0,
                )
                async with self._pool.acquire() as conn:
                    await conn.fetchval("SELECT 1")
                log.info(
                    "db.connected",
                    min_size=self._settings.pool_min_size,
                    max_size=self._settings.pool_max_size,
                )
                METRICS.service_up.labels(component="database").set(1)
                return
            except Exception as exc:
                last = exc
                METRICS.service_up.labels(component="database").set(0)
                log.warning("db.connect_retry", attempt=attempt, error=str(exc))
                await asyncio.sleep(backoff_s * attempt)
        raise RuntimeError(f"could not connect to the database: {last}")

    async def close(self) -> None:
        if self._pool is not None:
            await self._pool.close()
            self._pool = None
            METRICS.service_up.labels(component="database").set(0)
            log.info("db.closed")

    async def __aenter__(self) -> "Database":
        await self.connect()
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        await self.close()

    @property
    def pool(self) -> asyncpg.Pool:
        if self._pool is None:
            raise RuntimeError("database not connected")
        return self._pool

    @property
    def connected(self) -> bool:
        return self._pool is not None

    # --------------------------------------------------------------- queries

    async def fetch(self, query: str, *args: Any) -> list[asyncpg.Record]:
        async with self.pool.acquire() as conn:
            return await conn.fetch(query, *args)

    async def fetchrow(self, query: str, *args: Any) -> asyncpg.Record | None:
        async with self.pool.acquire() as conn:
            return await conn.fetchrow(query, *args)

    async def fetchval(self, query: str, *args: Any) -> Any:
        async with self.pool.acquire() as conn:
            return await conn.fetchval(query, *args)

    async def execute(self, query: str, *args: Any) -> str:
        async with self.pool.acquire() as conn:
            return await conn.execute(query, *args)

    async def executemany(self, query: str, rows: Sequence[Sequence[Any]]) -> None:
        """Batch write. Wrapped in one transaction so a batch is all-or-nothing."""
        if not rows:
            return
        async with self.pool.acquire() as conn:
            async with conn.transaction():
                await conn.executemany(query, rows)

    async def healthcheck(self) -> tuple[bool, float | None, str | None]:
        """Return (healthy, round-trip seconds, error).

        The latency is reported to the system screen as a measured number; on
        failure it is ``None`` so the UI shows N/A rather than a stale value.
        """
        if self._pool is None:
            return False, None, "pool not initialised"
        loop = asyncio.get_running_loop()
        started = loop.time()
        try:
            async with self._pool.acquire() as conn:
                await conn.fetchval("SELECT 1")
            elapsed = loop.time() - started
            METRICS.service_up.labels(component="database").set(1)
            return True, elapsed, None
        except Exception as exc:
            METRICS.service_up.labels(component="database").set(0)
            METRICS.errors.labels(component="database", kind=type(exc).__name__).inc()
            return False, None, str(exc)


_database: Database | None = None


def get_database(settings: DatabaseSettings | None = None) -> Database:
    """Process-wide singleton. Services create it once at startup."""
    global _database
    if _database is None:
        from racestream_common.config import get_settings

        _database = Database(settings or get_settings().db)
    return _database
