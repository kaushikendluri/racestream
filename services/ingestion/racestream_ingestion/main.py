"""Ingestion service entrypoint.

Two modes:

    python -m app.main --session 9472            ingest one session and exit
    python -m app.main --list --year 2024        list sessions available upstream

The service runs to completion and exits rather than staying resident: a
backfill is a job, not a daemon. Live polling against an in-progress session is
a separate mode and is not yet implemented, which the CLI says plainly rather
than pretending otherwise.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import sys

from racestream_common.config import get_settings
from racestream_common.db import Database
from racestream_common.kafka import EventProducer, ensure_topics
from racestream_common.obs import configure_logging, get_logger, start_metrics_server
from racestream_common.schemas import EventSource

from racestream_ingestion.openf1 import OpenF1Client
from racestream_ingestion.service import IngestionService

log = get_logger(__name__)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="racestream-ingestion",
        description="Ingest a Formula 1 session from the public OpenF1 API "
        "into Redpanda and TimescaleDB.",
    )
    parser.add_argument("--session", type=int, help="OpenF1 session_key to ingest")
    parser.add_argument(
        "--list", action="store_true", help="List sessions available upstream and exit"
    )
    parser.add_argument("--year", type=int, help="Filter --list by season")
    parser.add_argument(
        "--session-name",
        type=str,
        help="Filter --list by session name, e.g. Race, Qualifying",
    )
    parser.add_argument(
        "--window-seconds",
        type=int,
        default=300,
        help="Telemetry fetch window. 300s measures at ~22k records per request.",
    )
    parser.add_argument(
        "--max-windows",
        type=int,
        default=None,
        help="Stop after N telemetry windows. Useful for a quick smoke run.",
    )
    parser.add_argument(
        "--skip-telemetry",
        action="store_true",
        help="Ingest laps, timing and session data only; skip car telemetry and position.",
    )
    parser.add_argument(
        "--metrics-port",
        type=int,
        default=9101,
        help="Port to expose Prometheus metrics on.",
    )
    return parser


async def list_sessions(year: int | None, session_name: str | None) -> int:
    settings = get_settings()
    async with OpenF1Client(settings.openf1) as client:
        records = await client.sessions(year=year, session_name=session_name)

    if not records:
        print("No sessions matched.", file=sys.stderr)
        return 1

    records.sort(key=lambda r: (r.get("date_start") or ""))
    print(f"{'KEY':<8} {'YEAR':<6} {'COUNTRY':<22} {'CIRCUIT':<18} {'SESSION':<14} START")
    print("-" * 96)
    for record in records:
        print(
            f"{record.get('session_key', ''):<8} "
            f"{record.get('year', ''):<6} "
            f"{(record.get('country_name') or '')[:21]:<22} "
            f"{(record.get('circuit_short_name') or '')[:17]:<18} "
            f"{(record.get('session_name') or '')[:13]:<14} "
            f"{(record.get('date_start') or '')[:19]}"
        )
    print(f"\n{len(records)} session(s).")
    return 0


async def ingest(args: argparse.Namespace) -> int:
    settings = get_settings()
    await ensure_topics(settings.kafka.bootstrap_servers)

    database = Database(settings.db)
    await database.connect()

    try:
        async with OpenF1Client(settings.openf1) as client:
            async with EventProducer(settings.kafka, "ingestion") as producer:
                service = IngestionService(
                    settings=settings,
                    client=client,
                    producer=producer,
                    database=database,
                    window_seconds=args.window_seconds,
                )
                report = await service.ingest_session(
                    session_id=args.session,
                    source=EventSource.BACKFILL,
                    include_telemetry=not args.skip_telemetry,
                    max_windows=args.max_windows,
                )
    finally:
        await database.close()

    print(json.dumps(report.as_dict(), indent=2, default=str))
    # A partial ingest is a success with caveats, not a failure: the data that
    # arrived is real and usable. Only a total failure is a non-zero exit.
    return 0 if report.status in ("complete", "partial") else 1


async def main_async() -> int:
    args = build_parser().parse_args()
    settings = get_settings()

    configure_logging(
        service="ingestion",
        level=settings.obs.log_level,
        json_output=settings.obs.log_json,
        environment=settings.obs.environment,
    )

    if args.list:
        return await list_sessions(args.year, args.session_name)

    if args.session is None:
        build_parser().print_help(sys.stderr)
        print("\nerror: --session is required (or use --list)", file=sys.stderr)
        return 2

    try:
        start_metrics_server(args.metrics_port)
    except OSError as exc:
        # Losing the metrics endpoint should not abort an ingest.
        log.warning("ingest.metrics_port_unavailable", port=args.metrics_port, error=str(exc))

    return await ingest(args)


def main() -> None:
    try:
        raise SystemExit(asyncio.run(main_async()))
    except KeyboardInterrupt:
        print("\nInterrupted.", file=sys.stderr)
        raise SystemExit(130)


if __name__ == "__main__":
    main()
