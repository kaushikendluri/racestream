"""Event producer.

Responsibilities kept here so no service re-implements them: topic routing from
the event type, partition keying, trace stamping at produce time, metrics, and
a dead-letter path that never blocks the main flow.
"""

from __future__ import annotations

from types import TracebackType

from aiokafka import AIOKafkaProducer

from racestream_common.config import KafkaSettings
from racestream_common.obs import METRICS, get_logger
from racestream_common.obs.metrics import utcnow
from racestream_common.schemas import Event
from racestream_common.topics import DEAD_LETTER, topic_for
from racestream_common.kafka.serde import serialize_event

log = get_logger(__name__)


class EventProducer:
    """Async context manager wrapping :class:`AIOKafkaProducer`."""

    def __init__(self, settings: KafkaSettings, client_suffix: str = "producer") -> None:
        self._settings = settings
        self._client_id = f"{settings.client_id}-{client_suffix}"
        self._producer: AIOKafkaProducer | None = None

    async def start(self) -> None:
        self._producer = AIOKafkaProducer(
            bootstrap_servers=self._settings.bootstrap_servers,
            client_id=self._client_id,
            acks=self._settings.acks,
            linger_ms=self._settings.linger_ms,
            compression_type=self._settings.compression_type,
            max_batch_size=self._settings.max_batch_size,
            # Kafka's own idempotent producer: protects against duplicates
            # introduced by internal retries. Application-level idempotency is
            # still required for replay, and lives in the database upserts.
            enable_idempotence=self._settings.acks == "all",
        )
        await self._producer.start()
        log.info("producer.started", client_id=self._client_id)

    async def stop(self) -> None:
        if self._producer is not None:
            await self._producer.flush()
            await self._producer.stop()
            self._producer = None
            log.info("producer.stopped", client_id=self._client_id)

    async def __aenter__(self) -> "EventProducer":
        await self.start()
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        await self.stop()

    @property
    def started(self) -> bool:
        return self._producer is not None

    async def send(self, event: Event, wait: bool = False) -> None:
        """Publish one event to the topic its type routes to.

        ``wait=False`` returns as soon as the record is buffered, which is what
        the ingestion loop wants; the replay engine uses ``wait=True`` at
        checkpoints so that its progress reporting is honest.
        """
        if self._producer is None:
            raise RuntimeError("producer not started")

        topic = topic_for(event.event_type)
        stamped = event.model_copy(update={"trace": event.trace.stamp("produce_ts")})
        future = await self._producer.send(
            topic,
            value=serialize_event(stamped),
            key=stamped.partition_key,
        )
        if wait:
            await future
        METRICS.messages_produced.labels(
            topic=topic, event_type=stamped.event_type.value
        ).inc()

    async def send_many(self, events: list[Event]) -> int:
        """Publish a batch, isolating per-event failures.

        One bad event must not cost us the other 499 in the batch, so failures
        are counted and skipped rather than raised.
        """
        sent = 0
        for event in events:
            try:
                await self.send(event)
                sent += 1
            except Exception as exc:
                METRICS.errors.labels(component="producer", kind=type(exc).__name__).inc()
                METRICS.messages_dropped.labels(reason="produce_failed", stage="produce").inc()
                log.warning(
                    "producer.send_failed",
                    event_type=event.event_type.value,
                    session_id=event.session_id,
                    error=str(exc),
                )
        return sent

    async def send_to_dlq(self, raw: bytes, reason: str, origin_topic: str) -> None:
        """Park an unparseable message. Failures here are swallowed by design:
        losing a DLQ write must never take down the consumer."""
        if self._producer is None:
            return
        try:
            await self._producer.send(
                DEAD_LETTER.name,
                value=raw,
                headers=[
                    ("reason", reason.encode()[:512]),
                    ("origin_topic", origin_topic.encode()),
                    ("received_at", utcnow().isoformat().encode()),
                ],
            )
        except Exception as exc:
            log.error("producer.dlq_failed", error=str(exc), reason=reason)
