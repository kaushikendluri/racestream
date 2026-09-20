"""Session metadata persistence.

Why this bypasses Kafka
-----------------------
Telemetry streams; metadata does not. Sessions, drivers, teams and stints are
small, slow-changing reference data that the telemetry tables have a foreign key
onto - the session row must exist before a single telemetry row can be written.
Routing it through a topic would mean the processor had to guarantee ordering
between two topics just to satisfy a foreign key, which is a lot of machinery to
avoid one direct write of twenty rows.

So ingestion writes metadata directly and publishes ``session_meta`` to the
stream as well, so that a consumer reading only Kafka still learns what session
it is looking at. The duplication is deliberate and one-directional: the
database row is authoritative, the event is a description.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any

from racestream_common.db import Database
from racestream_common.obs import METRICS, get_logger

from racestream_ingestion.mappers import _int, _short_str, parse_timestamp

log = get_logger(__name__)


_UPSERT_SESSION = """
    INSERT INTO sessions (
        session_id, meeting_id, year, session_name, session_type,
        circuit_short_name, country_name, location, date_start, date_end,
        gmt_offset, ingest_status, updated_at
    )
    VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12, now())
    ON CONFLICT (session_id) DO UPDATE SET
        meeting_id         = EXCLUDED.meeting_id,
        year               = EXCLUDED.year,
        session_name       = EXCLUDED.session_name,
        session_type       = EXCLUDED.session_type,
        circuit_short_name = EXCLUDED.circuit_short_name,
        country_name       = EXCLUDED.country_name,
        location           = EXCLUDED.location,
        date_start         = EXCLUDED.date_start,
        date_end           = EXCLUDED.date_end,
        gmt_offset         = EXCLUDED.gmt_offset,
        ingest_status      = EXCLUDED.ingest_status,
        updated_at         = now()
"""

_UPSERT_DRIVER = """
    INSERT INTO drivers (
        session_id, driver_number, name_acronym, full_name, first_name,
        last_name, broadcast_name, country_code, team_name, team_colour,
        headshot_url
    )
    VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)
    ON CONFLICT (session_id, driver_number) DO UPDATE SET
        name_acronym   = EXCLUDED.name_acronym,
        full_name      = EXCLUDED.full_name,
        first_name     = EXCLUDED.first_name,
        last_name      = EXCLUDED.last_name,
        broadcast_name = EXCLUDED.broadcast_name,
        country_code   = EXCLUDED.country_code,
        team_name      = EXCLUDED.team_name,
        team_colour    = EXCLUDED.team_colour,
        headshot_url   = EXCLUDED.headshot_url
"""

_UPSERT_TEAM = """
    INSERT INTO teams (team_name, team_colour)
    VALUES ($1, $2)
    ON CONFLICT (team_name) DO UPDATE SET
        team_colour = COALESCE(EXCLUDED.team_colour, teams.team_colour)
"""

_UPSERT_STINT = """
    INSERT INTO stints (
        session_id, driver_number, stint_number, compound,
        lap_start, lap_end, tyre_age_at_start
    )
    VALUES ($1,$2,$3,$4,$5,$6,$7)
    ON CONFLICT (session_id, driver_number, stint_number) DO UPDATE SET
        compound          = COALESCE(EXCLUDED.compound, stints.compound),
        lap_start         = COALESCE(EXCLUDED.lap_start, stints.lap_start),
        lap_end           = COALESCE(EXCLUDED.lap_end, stints.lap_end),
        tyre_age_at_start = COALESCE(EXCLUDED.tyre_age_at_start, stints.tyre_age_at_start)
"""


class MetadataRepository:
    """Writes the relational tier and records what a session actually contains."""

    def __init__(self, database: Database) -> None:
        self._db = database

    async def upsert_session(
        self, record: dict[str, Any], status: str = "running"
    ) -> int:
        session_id = _int(record.get("session_key"))
        if session_id is None:
            raise ValueError("session record has no session_key")

        await self._db.execute(
            _UPSERT_SESSION,
            session_id,
            _int(record.get("meeting_key")),
            _int(record.get("year")),
            _short_str(record.get("session_name"), 128) or "Unknown",
            _short_str(record.get("session_type"), 64) or "Unknown",
            _short_str(record.get("circuit_short_name"), 128),
            _short_str(record.get("country_name"), 128),
            _short_str(record.get("location"), 128),
            parse_timestamp(record["date_start"]) if record.get("date_start") else None,
            parse_timestamp(record["date_end"]) if record.get("date_end") else None,
            _short_str(record.get("gmt_offset"), 32),
            status,
        )
        log.info("ingest.session_upserted", session_id=session_id, status=status)
        return session_id

    async def upsert_drivers(self, session_id: int, records: list[dict[str, Any]]) -> int:
        """Write the driver entry list, and the teams it references.

        Teams are written first: drivers reference a team by name, and writing
        them in the other order would briefly reference a team that does not yet
        exist for anything that later adds the foreign key.
        """
        teams: dict[str, str | None] = {}
        driver_rows: list[tuple] = []

        for record in records:
            number = _int(record.get("driver_number"))
            if number is None:
                continue
            team_name = _short_str(record.get("team_name"), 128)
            colour = _short_str(record.get("team_colour"), 16)
            if team_name:
                teams.setdefault(team_name, colour)
            driver_rows.append(
                (
                    session_id,
                    number,
                    _short_str(record.get("name_acronym"), 8),
                    _short_str(record.get("full_name"), 128),
                    _short_str(record.get("first_name"), 64),
                    _short_str(record.get("last_name"), 64),
                    _short_str(record.get("broadcast_name"), 128),
                    _short_str(record.get("country_code"), 8),
                    team_name,
                    colour,
                    _short_str(record.get("headshot_url"), 512),
                )
            )

        if teams:
            await self._db.executemany(_UPSERT_TEAM, [(n, c) for n, c in teams.items()])
        if driver_rows:
            await self._db.executemany(_UPSERT_DRIVER, driver_rows)
            METRICS.rows_written.labels(table="drivers").inc(len(driver_rows))

        log.info(
            "ingest.drivers_upserted",
            session_id=session_id,
            drivers=len(driver_rows),
            teams=len(teams),
        )
        return len(driver_rows)

    async def upsert_stints(self, session_id: int, records: list[dict[str, Any]]) -> int:
        rows = [
            (
                session_id,
                _int(r.get("driver_number")),
                _int(r.get("stint_number")),
                _short_str(r.get("compound"), 32),
                _int(r.get("lap_start")),
                _int(r.get("lap_end")),
                _int(r.get("tyre_age_at_start")),
            )
            for r in records
            if _int(r.get("driver_number")) is not None
            and _int(r.get("stint_number")) is not None
        ]
        if rows:
            await self._db.executemany(_UPSERT_STINT, rows)
            METRICS.rows_written.labels(table="stints").inc(len(rows))
        log.info("ingest.stints_upserted", session_id=session_id, stints=len(rows))
        return len(rows)

    async def record_availability(
        self,
        session_id: int,
        channels: dict[str, bool],
        total_laps: int | None,
        status: str,
    ) -> None:
        """Record which channels this session actually yielded.

        This is what lets the UI say "telemetry unavailable for this session" as
        a statement of fact rather than an inference from an empty query.
        """
        await self._db.execute(
            """
            UPDATE sessions SET
                has_car_telemetry = $2,
                has_position      = $3,
                has_laps          = $4,
                has_weather       = $5,
                has_race_control  = $6,
                total_laps        = COALESCE($7, total_laps),
                ingest_status     = $8,
                ingested_at       = now(),
                updated_at        = now()
            WHERE session_id = $1
            """,
            session_id,
            channels.get("car_telemetry", False),
            channels.get("position", False),
            channels.get("laps", False),
            channels.get("weather", False),
            channels.get("race_control", False),
            total_laps,
            status,
        )
        log.info(
            "ingest.availability_recorded",
            session_id=session_id,
            status=status,
            **{k: v for k, v in channels.items()},
        )

    async def session_window(
        self, session_id: int
    ) -> tuple[datetime | None, datetime | None]:
        row = await self._db.fetchrow(
            "SELECT date_start, date_end FROM sessions WHERE session_id = $1",
            session_id,
        )
        if row is None:
            return None, None
        return row["date_start"], row["date_end"]
