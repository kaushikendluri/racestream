"""Prometheus instrumentation shared by every service.

Cardinality policy
------------------
Labels are restricted to values from a small closed set: ``event_type`` (7),
``topic`` (5), ``stage`` (6), ``service`` (5), ``reason`` (a fixed enum).
``session_id`` and ``driver_id`` are deliberately *never* labels - a race
weekend would otherwise add thousands of series. Per-driver figures are served
from TimescaleDB through the API, which is the right tool for that question.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone

from prometheus_client import (
    CollectorRegistry,
    Counter,
    Gauge,
    Histogram,
    REGISTRY,
    start_http_server,
)

# Buckets tuned for a pipeline whose healthy end-to-end latency is tens of
# milliseconds but whose tail we care about out to several seconds.
_LATENCY_BUCKETS = (
    0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0
)


@dataclass(frozen=True)
class _Metrics:
    messages_ingested: Counter
    messages_produced: Counter
    messages_consumed: Counter
    messages_processed: Counter
    messages_dropped: Counter
    rows_written: Counter
    errors: Counter
    replay_events: Counter
    processing_latency: Histogram
    stage_latency: Histogram
    end_to_end_latency: Histogram
    db_write_latency: Histogram
    upstream_request_latency: Histogram
    consumer_lag: Gauge
    websocket_connections: Gauge
    queue_depth: Gauge
    service_up: Gauge
    replay_progress: Gauge


def _build(registry: CollectorRegistry) -> _Metrics:
    return _Metrics(
        messages_ingested=Counter(
            "racestream_messages_ingested_total",
            "Events accepted from the upstream source and validated.",
            ["event_type", "source"],
            registry=registry,
        ),
        messages_produced=Counter(
            "racestream_messages_produced_total",
            "Events successfully written to Kafka.",
            ["topic", "event_type"],
            registry=registry,
        ),
        messages_consumed=Counter(
            "racestream_messages_consumed_total",
            "Records read from Kafka by a consumer group.",
            ["topic", "group"],
            registry=registry,
        ),
        messages_processed=Counter(
            "racestream_messages_processed_total",
            "Events that completed processing and were queued for persistence.",
            ["event_type"],
            registry=registry,
        ),
        messages_dropped=Counter(
            "racestream_messages_dropped_total",
            "Events discarded, labelled by why. Every drop is counted somewhere.",
            ["reason", "stage"],
            registry=registry,
        ),
        rows_written=Counter(
            "racestream_rows_written_total",
            "Rows committed to TimescaleDB.",
            ["table"],
            registry=registry,
        ),
        errors=Counter(
            "racestream_errors_total",
            "Handled errors by component and class.",
            ["component", "kind"],
            registry=registry,
        ),
        replay_events=Counter(
            "racestream_replay_events_total",
            "Events emitted by the replay engine.",
            ["event_type"],
            registry=registry,
        ),
        processing_latency=Histogram(
            "racestream_processing_latency_seconds",
            "Time spent inside the stream processor for one event.",
            ["event_type"],
            buckets=_LATENCY_BUCKETS,
            registry=registry,
        ),
        stage_latency=Histogram(
            "racestream_stage_latency_seconds",
            "Time between two adjacent pipeline stages, from event trace stamps.",
            ["stage"],
            buckets=_LATENCY_BUCKETS,
            registry=registry,
        ),
        end_to_end_latency=Histogram(
            "racestream_end_to_end_latency_seconds",
            "Ingest timestamp to WebSocket send, measured from trace stamps.",
            ["event_type"],
            buckets=_LATENCY_BUCKETS,
            registry=registry,
        ),
        db_write_latency=Histogram(
            "racestream_db_write_latency_seconds",
            "Wall time of one batch COPY/INSERT.",
            ["table"],
            buckets=_LATENCY_BUCKETS,
            registry=registry,
        ),
        upstream_request_latency=Histogram(
            "racestream_upstream_request_latency_seconds",
            "Latency of a request to the upstream data source.",
            ["endpoint", "outcome"],
            buckets=_LATENCY_BUCKETS,
            registry=registry,
        ),
        consumer_lag=Gauge(
            "racestream_consumer_lag",
            "Records between a consumer group's committed offset and the log end.",
            ["topic", "group"],
            registry=registry,
        ),
        websocket_connections=Gauge(
            "racestream_websocket_connections",
            "Currently open WebSocket connections.",
            registry=registry,
        ),
        queue_depth=Gauge(
            "racestream_queue_depth",
            "Occupancy of a bounded internal queue. The backpressure signal.",
            ["queue"],
            registry=registry,
        ),
        service_up=Gauge(
            "racestream_service_up",
            "1 when the service considers itself healthy, 0 when degraded.",
            ["component"],
            registry=registry,
        ),
        replay_progress=Gauge(
            "racestream_replay_progress_ratio",
            "Fraction of the replay window already emitted, 0..1.",
            registry=registry,
        ),
    )


METRICS = _build(REGISTRY)


def start_metrics_server(port: int) -> None:
    """Expose /metrics on ``port``. Safe to call once per process."""
    start_http_server(port)


_STAGE_PAIRS: tuple[tuple[str, str, str], ...] = (
    ("source_to_ingest", "source_ts", "ingest_ts"),
    ("ingest_to_produce", "ingest_ts", "produce_ts"),
    ("produce_to_consume", "produce_ts", "consume_ts"),
    ("consume_to_process", "consume_ts", "process_ts"),
    ("process_to_db", "process_ts", "db_ts"),
    ("process_to_ws", "process_ts", "ws_ts"),
)


def observe_pipeline_latency(trace, event_type: str) -> None:
    """Record every stage gap that is actually measurable on this event.

    Unstamped stages are skipped rather than defaulted, so the histograms only
    ever contain real measurements.
    """
    for label, start, end in _STAGE_PAIRS:
        seconds = trace.elapsed_seconds(start, end)
        if seconds is not None and seconds >= 0:
            METRICS.stage_latency.labels(stage=label).observe(seconds)

    e2e = trace.elapsed_seconds("ingest_ts", "ws_ts")
    if e2e is not None and e2e >= 0:
        METRICS.end_to_end_latency.labels(event_type=event_type).observe(e2e)


def utcnow() -> datetime:
    return datetime.now(timezone.utc)
