#!/usr/bin/env python
"""Rate-controlled pipeline benchmark.

Why a rate limiter is the whole point
-------------------------------------
Publishing as fast as possible measures how fast the *producer* is, and the
latency it reports is dominated by queueing: an event that waits 78 seconds in
a backlog genuinely took 78 seconds, but that number describes a saturated
pipeline, not a live one. Quoting it as "end-to-end latency" would be
meaningless at best.

So this harness publishes at a *target rate* and reports the latency observed at
that rate. Running it across several rates shows where the pipeline stops
keeping up, which is the figure that actually matters.

For reference, a real F1 session produces roughly:

    20 drivers x 3.9 Hz (car_data)  ~=  78 events/s
    20 drivers x 3.7 Hz (location)  ~=  74 events/s
                                       ----------
                                        ~152 events/s

Measured on live OpenF1 data, not assumed.

Usage
-----
    python load/run_benchmark.py --rate 500 --duration 60
    python load/run_benchmark.py --sweep 250,500,1000,2000 --duration 45
"""

from __future__ import annotations

import argparse
import asyncio
import json
import statistics
import sys
import time
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT / "services" / "common"))

from racestream_common.config import DatabaseSettings, KafkaSettings  # noqa: E402
from racestream_common.db import Database  # noqa: E402
from racestream_common.kafka import EventProducer, ensure_topics  # noqa: E402
from racestream_common.obs import configure_logging, get_logger  # noqa: E402
from racestream_common.schemas import (  # noqa: E402
    CarTelemetry,
    Event,
    EventSource,
    EventType,
    PipelineTrace,
    Position,
)

log = get_logger(__name__)


@dataclass
class RunResult:
    """One measured run. Every field is observed, never projected."""

    run_label: str
    target_rate: int
    duration_s: float
    published: int
    achieved_rate: float
    samples: int
    drain_s: float | None
    p50_ms: float | None
    p95_ms: float | None
    p99_ms: float | None
    max_ms: float | None
    kafka_p50_ms: float | None
    derive_p50_ms: float | None
    db_p50_ms: float | None
    peak_lag: int
    kept_up: bool


async def load_source_rows(db: Database, session_id: int, limit: int) -> list[dict]:
    """Take real telemetry out of TimescaleDB to replay through the pipeline.

    Real rows rather than generated ones: synthetic uniform data compresses
    differently, hashes differently across partitions, and would not exercise
    the same validation paths.
    """
    rows = await db.fetch(
        """
        SELECT driver_number, sample_time, speed, throttle, brake, n_gear, rpm, drs
        FROM car_telemetry
        WHERE session_id = $1
        ORDER BY sample_time
        LIMIT $2
        """,
        session_id,
        limit,
    )
    if not rows:
        raise SystemExit(
            f"No telemetry for session {session_id}. Ingest it first:\n"
            f"  python -m racestream_ingestion.main --session {session_id}"
        )
    return [dict(r) for r in rows]


def build_event(row: dict, session_id: int, now: datetime) -> Event:
    """Build an event stamped with *wall-clock* ingest time.

    The original sample_time is kept as the event timestamp so the data stays
    true to its source, but ``ingest_ts`` is now, because that is when this event
    entered the pipeline. Measuring latency from a 2024 timestamp would report
    a latency of two years.
    """
    return Event(
        event_type=EventType.CAR_TELEMETRY,
        source=EventSource.REPLAY,
        session_id=session_id,
        driver_id=row["driver_number"],
        timestamp=row["sample_time"],
        payload=CarTelemetry(
            speed=row["speed"],
            throttle=row["throttle"],
            brake=row["brake"],
            n_gear=row["n_gear"],
            rpm=row["rpm"],
            drs=row["drs"],
        ),
        trace=PipelineTrace(source_ts=row["sample_time"]).stamp("ingest_ts", now),
    )


async def measure_lag(kafka: KafkaSettings, group: str = "racestream-telemetry") -> int:
    """Total lag for a consumer group, read directly from the broker."""
    from aiokafka.admin import AIOKafkaAdminClient

    admin = AIOKafkaAdminClient(bootstrap_servers=kafka.bootstrap_servers)
    await admin.start()
    try:
        from aiokafka import AIOKafkaConsumer

        consumer = AIOKafkaConsumer(
            bootstrap_servers=kafka.bootstrap_servers, group_id=None
        )
        await consumer.start()
        try:
            offsets = await admin.list_consumer_group_offsets(group)
            if not offsets:
                return 0
            partitions = list(offsets)
            end = await consumer.end_offsets(partitions)
            return sum(
                max(0, end.get(tp, 0) - meta.offset)
                for tp, meta in offsets.items()
                if meta.offset >= 0
            )
        finally:
            await consumer.stop()
    except Exception:
        return 0
    finally:
        await admin.close()


async def run_once(
    db: Database,
    kafka: KafkaSettings,
    rows: list[dict],
    session_id: int,
    target_rate: int,
    duration_s: float,
    run_label: str,
) -> RunResult:
    """Publish at ``target_rate`` for ``duration_s``, then wait for the drain."""
    total = int(target_rate * duration_s)
    interval = 1.0 / target_rate

    # Latency rows are written by the *processor*, which knows nothing about
    # this run and so cannot tag them with run_label from here. The run is
    # bounded by its own observed time window instead, which is also the more
    # honest filter: it captures exactly the events this run caused.
    window_start = datetime.now(timezone.utc)

    print(f"\n  rate {target_rate:>5}/s for {duration_s:.0f}s  ({total:,} events)")

    peak_lag = 0
    published = 0
    started = time.perf_counter()

    async with EventProducer(kafka, f"bench-{target_rate}") as producer:
        next_send = started
        for i in range(total):
            row = rows[i % len(rows)]
            await producer.send(build_event(row, session_id, datetime.now(timezone.utc)))
            published += 1

            # Pace by absolute schedule rather than sleeping a fixed interval,
            # so that time spent in send() does not accumulate as drift.
            next_send += interval
            delay = next_send - time.perf_counter()
            if delay > 0:
                await asyncio.sleep(delay)

            if i % max(1, target_rate) == 0:
                peak_lag = max(peak_lag, await measure_lag(kafka))

        await producer.flush()

    publish_elapsed = time.perf_counter() - started
    achieved = published / publish_elapsed if publish_elapsed > 0 else 0.0
    print(f"    published {published:,} in {publish_elapsed:.1f}s -> {achieved:.0f}/s")

    # Wait for the consumer to catch up, so the percentiles cover every event.
    drain_started = time.perf_counter()
    drain_s: float | None = None
    for _ in range(120):
        lag = await measure_lag(kafka)
        peak_lag = max(peak_lag, lag)
        if lag == 0:
            drain_s = time.perf_counter() - drain_started
            break
        await asyncio.sleep(1.0)
    print(f"    drained in {drain_s:.1f}s" if drain_s is not None else "    did not drain in 120s")

    # Give the writer's periodic flush time to persist the final latency rows.
    await asyncio.sleep(2.0)

    stats = await db.fetchrow(
        """
        SELECT count(*) AS samples,
               percentile_cont(0.50) WITHIN GROUP (ORDER BY end_to_end_s)       AS p50,
               percentile_cont(0.95) WITHIN GROUP (ORDER BY end_to_end_s)       AS p95,
               percentile_cont(0.99) WITHIN GROUP (ORDER BY end_to_end_s)       AS p99,
               max(end_to_end_s)                                                AS worst,
               percentile_cont(0.50) WITHIN GROUP (ORDER BY produce_to_consume_s) AS kafka_p50,
               percentile_cont(0.50) WITHIN GROUP (ORDER BY consume_to_process_s) AS derive_p50,
               percentile_cont(0.50) WITHIN GROUP (ORDER BY process_to_db_s)      AS db_p50
        FROM pipeline_events
        WHERE observed_at >= $1 AND end_to_end_s IS NOT NULL
        """,
        window_start,
    )

    def ms(value) -> float | None:
        return round(float(value) * 1000, 2) if value is not None else None

    return RunResult(
        run_label=run_label,
        target_rate=target_rate,
        duration_s=round(publish_elapsed, 2),
        published=published,
        achieved_rate=round(achieved, 1),
        samples=stats["samples"] or 0,
        drain_s=round(drain_s, 2) if drain_s is not None else None,
        p50_ms=ms(stats["p50"]),
        p95_ms=ms(stats["p95"]),
        p99_ms=ms(stats["p99"]),
        max_ms=ms(stats["worst"]),
        kafka_p50_ms=ms(stats["kafka_p50"]),
        derive_p50_ms=ms(stats["derive_p50"]),
        db_p50_ms=ms(stats["db_p50"]),
        peak_lag=peak_lag,
        # "Kept up" means the producer sustained the target rate and the
        # consumer drained promptly afterwards. Anything else is saturation.
        kept_up=achieved >= target_rate * 0.95 and (drain_s is not None and drain_s < 10),
    )


def print_table(results: list[RunResult]) -> None:
    print("\n" + "=" * 100)
    print("MEASURED RESULTS  (every figure observed; blank means not measurable)")
    print("=" * 100)
    header = (
        f"{'TARGET':>7} {'ACHIEVED':>9} {'SAMPLES':>8} {'p50':>9} {'p95':>9} "
        f"{'p99':>9} {'max':>9} {'PEAK LAG':>9} {'KEPT UP':>8}"
    )
    print(header)
    print("-" * 100)
    for r in results:
        def f(v):
            return f"{v:.1f}ms" if v is not None else "n/a"
        print(
            f"{r.target_rate:>7} {r.achieved_rate:>9.0f} {r.samples:>8} "
            f"{f(r.p50_ms):>9} {f(r.p95_ms):>9} {f(r.p99_ms):>9} {f(r.max_ms):>9} "
            f"{r.peak_lag:>9} {('yes' if r.kept_up else 'NO'):>8}"
        )
    print("-" * 100)
    print("\nStage breakdown (p50):")
    print(f"{'TARGET':>7} {'kafka':>10} {'derive':>10} {'db write':>10}")
    for r in results:
        def f(v):
            return f"{v:.1f}ms" if v is not None else "n/a"
        print(f"{r.target_rate:>7} {f(r.kafka_p50_ms):>10} {f(r.derive_p50_ms):>10} {f(r.db_p50_ms):>10}")


async def main_async() -> int:
    parser = argparse.ArgumentParser(description="RaceStream pipeline benchmark")
    parser.add_argument("--session", type=int, default=9472)
    parser.add_argument("--rate", type=int, default=500, help="Events per second")
    parser.add_argument("--sweep", type=str, help="Comma-separated rates, e.g. 250,500,1000")
    parser.add_argument("--duration", type=float, default=45.0)
    parser.add_argument("--db-dsn", type=str, default=None)
    parser.add_argument("--bootstrap", type=str, default="localhost:19092")
    parser.add_argument("--json-out", type=str, default=None)
    args = parser.parse_args()

    configure_logging("benchmark", level="WARNING", json_output=False)

    db_settings = DatabaseSettings(
        dsn=args.db_dsn or "postgresql://racestream:racestream@localhost:5434/racestream"
    )
    kafka = KafkaSettings(bootstrap_servers=args.bootstrap)
    await ensure_topics(kafka.bootstrap_servers)

    rates = (
        [int(r) for r in args.sweep.split(",")] if args.sweep else [args.rate]
    )

    async with Database(db_settings) as db:
        rows = await load_source_rows(db, args.session, limit=20000)
        print(f"Source: {len(rows):,} real telemetry rows from session {args.session}")
        print(f"Reference: a live F1 session produces ~152 events/s (measured)")

        results: list[RunResult] = []
        for rate in rates:
            label = f"bench-{rate}-{int(time.time())}"
            results.append(
                await run_once(db, kafka, rows, args.session, rate, args.duration, label)
            )
            # Let the system settle between runs so one does not bleed into the next.
            if rate != rates[-1]:
                await asyncio.sleep(5)

    print_table(results)

    if args.json_out:
        Path(args.json_out).write_text(
            json.dumps([asdict(r) for r in results], indent=2), encoding="utf-8"
        )
        print(f"\nWrote {args.json_out}")

    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main_async()))
