"""Shared mutable state for the API process.

Held on ``app.state`` rather than in module globals so that a test can build an
app instance with its own dependencies, and so ownership of the database pool,
the live hub and its Kafka consumer is explicit.
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from racestream_common.db import Database

if TYPE_CHECKING:  # avoid a circular import at runtime
    from racestream_api.hub import LiveHub
    from racestream_common.kafka import EventConsumer, EventProducer


@dataclass
class AppState:
    database: Database | None = None
    hub: "LiveHub | None" = None
    consumer: "EventConsumer | None" = None
    dlq_producer: "EventProducer | None" = None
    started_at: float = field(default_factory=time.monotonic)

    @property
    def uptime_seconds(self) -> float:
        return time.monotonic() - self.started_at
