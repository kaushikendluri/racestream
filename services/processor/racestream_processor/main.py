"""Stream processor entrypoint.

Unlike ingestion, this is a long-running service: it stays subscribed and
processes whatever arrives, whether that is a live ingest or a replay.

    python -m racestream_processor.main
"""

from __future__ import annotations

import argparse
import asyncio
import signal

from racestream_common.config import get_settings
from racestream_common.db import Database
from racestream_common.kafka import ensure_topics
from racestream_common.obs import configure_logging, get_logger, start_metrics_server

from racestream_processor.service import StreamProcessor

log = get_logger(__name__)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="racestream-processor",
        description="Consume RaceStream topics, derive session state, persist to TimescaleDB.",
    )
    parser.add_argument("--metrics-port", type=int, default=9102)
    parser.add_argument(
        "--run-label",
        type=str,
        default=None,
        help="Tag persisted latency samples with a benchmark run name.",
    )
    return parser


async def main_async() -> int:
    args = build_parser().parse_args()
    settings = get_settings()

    configure_logging(
        service="processor",
        level=settings.obs.log_level,
        json_output=settings.obs.log_json,
        environment=settings.obs.environment,
    )

    try:
        start_metrics_server(args.metrics_port)
    except OSError as exc:
        log.warning("processor.metrics_port_unavailable", port=args.metrics_port, error=str(exc))

    await ensure_topics(settings.kafka.bootstrap_servers)

    database = Database(settings.db)
    await database.connect()

    processor = StreamProcessor(settings, database, run_label=args.run_label)
    await processor.start()

    # Shut down on SIGTERM as well as SIGINT: a container stop sends SIGTERM,
    # and an unclean exit there would leave the last batch unwritten.
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        try:
            loop.add_signal_handler(sig, stop.set)
        except NotImplementedError:
            # Windows does not support add_signal_handler for these; the
            # KeyboardInterrupt path in main() covers it there.
            pass

    log.info("processor.ready")
    try:
        await stop.wait()
    finally:
        await processor.stop()
        await database.close()
    return 0


def main() -> None:
    try:
        raise SystemExit(asyncio.run(main_async()))
    except KeyboardInterrupt:
        raise SystemExit(130)


if __name__ == "__main__":
    main()
