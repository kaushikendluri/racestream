"""API response models.

Every endpoint declares a response model. That is what keeps the OpenAPI schema
truthful and stops an internal database row shape leaking out of the service by
accident.

Convention throughout: a field that is genuinely unknown is ``None``, and the
frontend renders ``N/A``. No endpoint substitutes a zero or a placeholder for a
measurement that was not taken.
"""

from __future__ import annotations

from datetime import datetime
from enum import Enum
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field


class HealthState(str, Enum):
    HEALTHY = "healthy"
    DEGRADED = "degraded"
    DOWN = "down"
    UNKNOWN = "unknown"


class ComponentHealth(BaseModel):
    """One row of the System Health screen."""

    model_config = ConfigDict(extra="forbid")

    name: str
    state: HealthState
    detail: str | None = Field(
        default=None, description="Human-readable reason, shown on hover."
    )
    latency_ms: float | None = Field(
        default=None, description="Measured round-trip. None when not measurable."
    )
    checked_at: datetime


class HealthResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    state: HealthState = Field(
        description="Worst state across components: the whole is only as healthy as its parts."
    )
    version: str
    environment: str
    uptime_seconds: float
    components: list[ComponentHealth]


class ReadinessResponse(BaseModel):
    """Distinct from liveness.

    Liveness asks "is the process alive" (restart me if not). Readiness asks
    "can I serve traffic" (route around me if not). Conflating them causes a
    container to be killed for a dependency outage it would recover from.
    """

    model_config = ConfigDict(extra="forbid")

    ready: bool
    checks: dict[str, bool]
    detail: str | None = None


class SessionSummary(BaseModel):
    """A row in the Session Explorer."""

    model_config = ConfigDict(extra="forbid")

    session_id: int
    year: int | None = None
    session_name: str
    session_type: str
    circuit_short_name: str | None = None
    country_name: str | None = None
    location: str | None = None
    date_start: datetime | None = None
    date_end: datetime | None = None
    total_laps: int | None = None
    ingest_status: str
    ingested_at: datetime | None = None
    # Drives the "telemetry unavailable for this session" empty states, so that
    # the message is a fact about the data rather than a guess.
    available_channels: dict[str, bool]
    replay_available: bool = Field(
        description="True only when enough has been ingested for a replay to mean anything."
    )


class DriverInfo(BaseModel):
    model_config = ConfigDict(extra="forbid")

    driver_number: int
    name_acronym: str | None = None
    full_name: str | None = None
    broadcast_name: str | None = None
    country_code: str | None = None
    team_name: str | None = None
    team_colour: str | None = None
    headshot_url: str | None = None


class PaginationMeta(BaseModel):
    model_config = ConfigDict(extra="forbid")

    limit: int
    offset: int
    returned: int
    total: int | None = Field(
        default=None,
        description="Omitted on large time-series scans where COUNT would cost more than the query.",
    )


class ErrorResponse(BaseModel):
    """Uniform error body, so the frontend has one shape to handle."""

    model_config = ConfigDict(extra="forbid")

    error: str = Field(description="Stable machine-readable code.")
    detail: str = Field(description="Human-readable explanation.")
    status_code: int
    path: str | None = None
