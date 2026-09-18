"""Topic creation.

Topics are created explicitly at startup rather than relying on Kafka's
auto-creation, because auto-created topics get the broker default partition
count - which would silently discard the partitioning strategy in topics.py.
"""

from __future__ import annotations

from aiokafka.admin import AIOKafkaAdminClient, NewTopic
from aiokafka.errors import TopicAlreadyExistsError

from racestream_common.obs import get_logger
from racestream_common.topics import ALL_TOPICS, TopicSpec

log = get_logger(__name__)


async def ensure_topics(
    bootstrap_servers: str, specs: tuple[TopicSpec, ...] = ALL_TOPICS
) -> None:
    """Create any missing topics. Existing topics are left untouched."""
    admin = AIOKafkaAdminClient(bootstrap_servers=bootstrap_servers)
    await admin.start()
    try:
        existing = set(await admin.list_topics())
        missing = [s for s in specs if s.name not in existing]
        if not missing:
            log.info("topics.ready", count=len(specs))
            return
        await admin.create_topics(
            [
                NewTopic(
                    name=s.name,
                    num_partitions=s.partitions,
                    replication_factor=s.replication_factor,
                    topic_configs={"retention.ms": str(s.retention_ms)},
                )
                for s in missing
            ]
        )
        log.info("topics.created", topics=[s.name for s in missing])
    except TopicAlreadyExistsError:
        # Two services racing to start is expected and benign.
        log.info("topics.already_exist")
    finally:
        await admin.close()
