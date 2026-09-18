"""Event consumer with explicit offset control, backpressure and a DLQ.

Delivery semantics: at-least-once. Offsets are committed only after a batch has
been durably handled, so a crash replays the tail of the log. Duplicates are
absorbed by the idempotent upserts in the database layer, which is what makes
the combination behave as effectively-once. See docs/engineering-decisions.md.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from types import TracebackType
from typing import Awaitable, Callable, Sequence

from aiokafka import AIOKafkaConsumer, TopicPartition
from aiokafka.structs import ConsumerRecord

from racestream_common.config import KafkaSettings
from racestream_common.kafka.producer import EventProducer
from racestream_common.kafka.serde import DeserializationError, deserialize_event
from racestream_common.obs import METRICS, get_logger
from racestream_common.obs.metrics import utcnow
from racestream_common.schemas import Event

log = get_logger(__name__)

BatchHandler = Callable[[list[Event]], Awaitable[None]]


@dataclass
class ConsumerStats:
    """Counters the chaos and system screens read to describe a consumer honestly."""

    group_id: str
    topics: tuple[str, ...]
    state: str = "stopped"
    consumed: int = 0
    processed: int = 0
    dead_lettered: int = 0
    batches: int = 0
    last_error: str | None = None
    started_at: str | None = None
    last_message_at: str | None = None
    lag_by_topic: dict[str, int] = field(default_factory=dict)

    def as_dict(self) -> dict:
        return {
            "group_id": self.group_id,
            "topics": list(self.topics),
            "state": self.state,
            "consumed": self.consumed,
            "processed": self.processed,
            "dead_lettered": self.dead_lettered,
            "batches": self.batches,
            "last_error": self.last_error,
            "started_at": self.started_at,
            "last_message_at": self.last_message_at,
            "lag_by_topic": dict(self.lag_by_topic),
            "total_lag": sum(self.lag_by_topic.values()) if self.lag_by_topic else 0,
        }


class EventConsumer:
    """Consume a set of topics and hand validated batches to ``handler``.

    Backpressure: the fetch is bounded by ``max_poll_records``, and because the
    loop awaits ``handler(...)`` before polling again, a slow handler simply
    stops fetching. Lag then accumulates on the broker - which is visible,
    bounded by retention, and exactly the signal we want - instead of an
    unbounded in-process buffer growing until the container is OOM-killed.
    """

    def __init__(
        self,
        settings: KafkaSettings,
        topics: Sequence[str],
        group_id: str,
        handler: BatchHandler,
        dlq_producer: EventProducer | None = None,
        poll_timeout_ms: int = 1000,
    ) -> None:
        self._settings = settings
        self._topics = tuple(topics)
        self._group_id = group_id
        self._handler = handler
        self._dlq = dlq_producer
        self._poll_timeout_ms = poll_timeout_ms

        self._consumer: AIOKafkaConsumer | None = None
        self._task: asyncio.Task | None = None
        self._stop_event = asyncio.Event()
        self._resume_event = asyncio.Event()
        self._resume_event.set()
        self.stats = ConsumerStats(group_id=group_id, topics=self._topics)

    # ---------------------------------------------------------------- lifecycle

    async def start(self) -> None:
        if self._task is not None:
            return
        self._consumer = AIOKafkaConsumer(
            *self._topics,
            bootstrap_servers=self._settings.bootstrap_servers,
            group_id=self._group_id,
            client_id=f"{self._settings.client_id}-{self._group_id}",
            enable_auto_commit=False,
            auto_offset_reset=self._settings.auto_offset_reset,
            max_poll_records=self._settings.max_poll_records,
            session_timeout_ms=self._settings.session_timeout_ms,
        )
        await self._consumer.start()
        self._stop_event.clear()
        self.stats.state = "running"
        self.stats.started_at = utcnow().isoformat()
        self._task = asyncio.create_task(self._run(), name=f"consumer-{self._group_id}")
        log.info("consumer.started", group=self._group_id, topics=list(self._topics))

    async def stop(self) -> None:
        self._stop_event.set()
        self._resume_event.set()
        if self._task is not None:
            try:
                await asyncio.wait_for(self._task, timeout=15)
            except (asyncio.TimeoutError, asyncio.CancelledError):
                self._task.cancel()
            self._task = None
        if self._consumer is not None:
            await self._consumer.stop()
            self._consumer = None
        self.stats.state = "stopped"
        log.info("consumer.stopped", group=self._group_id, consumed=self.stats.consumed)

    async def __aenter__(self) -> "EventConsumer":
        await self.start()
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        await self.stop()

    # ------------------------------------------------------------ chaos controls

    def pause(self) -> None:
        """Stop fetching without leaving the group.

        Group membership is kept, so no rebalance occurs and lag grows against a
        still-live consumer - the cleanest way to demonstrate backpressure.
        """
        self._resume_event.clear()
        self.stats.state = "paused"
        log.warning("consumer.paused", group=self._group_id)

    def resume(self) -> None:
        self._resume_event.set()
        self.stats.state = "running"
        log.info("consumer.resumed", group=self._group_id)

    @property
    def running(self) -> bool:
        return self._task is not None and not self._task.done()

    # ----------------------------------------------------------------- main loop

    async def _run(self) -> None:
        assert self._consumer is not None
        while not self._stop_event.is_set():
            try:
                await self._resume_event.wait()
                if self._stop_event.is_set():
                    break

                batches = await self._consumer.getmany(
                    timeout_ms=self._poll_timeout_ms,
                    max_records=self._settings.max_poll_records,
                )
                if not batches:
                    await self._refresh_lag()
                    continue

                events: list[Event] = []
                for tp, records in batches.items():
                    METRICS.messages_consumed.labels(
                        topic=tp.topic, group=self._group_id
                    ).inc(len(records))
                    self.stats.consumed += len(records)
                    for record in records:
                        event = await self._decode(record)
                        if event is not None:
                            events.append(event)

                if events:
                    await self._handler(events)
                    self.stats.processed += len(events)
                    self.stats.last_message_at = utcnow().isoformat()

                # Commit only after the handler returned: at-least-once.
                await self._consumer.commit()
                self.stats.batches += 1
                await self._refresh_lag()

            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self.stats.last_error = f"{type(exc).__name__}: {exc}"
                self.stats.state = "degraded"
                METRICS.errors.labels(
                    component=f"consumer.{self._group_id}", kind=type(exc).__name__
                ).inc()
                log.error("consumer.loop_error", group=self._group_id, error=str(exc))
                # Back off briefly so a persistent fault does not spin the CPU.
                await asyncio.sleep(1.0)
                self.stats.state = "running"

    async def _decode(self, record: ConsumerRecord) -> Event | None:
        """Validate one record, routing poison messages to the DLQ.

        A single malformed message must never stall a partition, so the failure
        is recorded and the offset moves on.
        """
        try:
            event = deserialize_event(record.value)
        except DeserializationError as exc:
            self.stats.dead_lettered += 1
            METRICS.messages_dropped.labels(
                reason="invalid_schema", stage="consume"
            ).inc()
            log.warning(
                "consumer.poison_message",
                group=self._group_id,
                topic=record.topic,
                partition=record.partition,
                offset=record.offset,
                error=str(exc),
            )
            if self._dlq is not None:
                await self._dlq.send_to_dlq(exc.raw, str(exc), record.topic)
            return None
        return event.model_copy(update={"trace": event.trace.stamp("consume_ts")})

    async def _refresh_lag(self) -> None:
        """Publish per-topic consumer lag.

        Computed as (log end offset - current position) summed over the
        partitions this instance owns. With several instances in a group each
        reports its own share, and the Grafana panel sums them.
        """
        if self._consumer is None:
            return
        try:
            assignment: set[TopicPartition] = self._consumer.assignment()
            if not assignment:
                return
            end_offsets = await self._consumer.end_offsets(list(assignment))
            per_topic: dict[str, int] = {t: 0 for t in self._topics}
            for tp in assignment:
                position = await self._consumer.position(tp)
                lag = max(0, end_offsets.get(tp, position) - position)
                per_topic[tp.topic] = per_topic.get(tp.topic, 0) + lag
            for topic, lag in per_topic.items():
                METRICS.consumer_lag.labels(topic=topic, group=self._group_id).set(lag)
            self.stats.lag_by_topic = per_topic
        except Exception as exc:
            # Lag reporting is diagnostics; never let it break consumption.
            log.debug(
                "consumer.lag_refresh_failed", group=self._group_id, error=str(exc)
            )
