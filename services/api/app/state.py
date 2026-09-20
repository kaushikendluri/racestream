"""Shared mutable state for the API process.

Held on ``app.state`` rather than in module globals so that a test can build an
app instance with its own dependencies, and so ownership of the database pool
and the live-stream hub is explicit.
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field

from racestream_common.db import Database


@dataclass
class AppState:
    database: Database | None = None
    started_at: float = field(default_factory=time.monotonic)

    @property
    def uptime_seconds(self) -> float:
        return time.monotonic() - self.started_at
