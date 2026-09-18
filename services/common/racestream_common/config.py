"""Environment-driven configuration.

Nothing in RaceStream reads ``os.environ`` directly; everything goes through a
settings object so that defaults, types and validation live in one place and a
misconfigured deployment fails at startup rather than on the first message.
"""

from __future__ import annotations

from functools import lru_cache

from pydantic import Field, PostgresDsn, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class _Base(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env", env_file_encoding="utf-8", extra="ignore"
    )


class KafkaSettings(_Base):
    model_config = SettingsConfigDict(env_prefix="KAFKA_", extra="ignore")

    bootstrap_servers: str = Field(default="localhost:19092")
    client_id: str = Field(default="racestream")
    # Batch produce: latency is dominated by the source poll interval, not by a
    # few ms of linger, and batching cuts broker round-trips substantially.
    linger_ms: int = Field(default=10, ge=0, le=1000)
    compression_type: str | None = Field(default="lz4")
    acks: str = Field(default="all", description="'all' for durability; 1 for throughput tests")
    max_batch_size: int = Field(default=64 * 1024, ge=1024)
    # Consumer side
    max_poll_records: int = Field(default=500, ge=1, le=10000)
    session_timeout_ms: int = Field(default=30000, ge=6000)
    auto_offset_reset: str = Field(default="earliest")

    @field_validator("acks")
    @classmethod
    def _valid_acks(cls, v: str) -> str:
        if v not in ("0", "1", "all"):
            raise ValueError("KAFKA_ACKS must be one of '0', '1', 'all'")
        return v


class DatabaseSettings(_Base):
    model_config = SettingsConfigDict(env_prefix="DB_", extra="ignore")

    dsn: PostgresDsn = Field(
        default="postgresql://racestream:racestream@localhost:5432/racestream"
    )
    pool_min_size: int = Field(default=2, ge=1)
    pool_max_size: int = Field(default=10, ge=1)
    command_timeout: float = Field(default=30.0, gt=0)
    # Writes are batched; these bound how long an event can sit unwritten.
    write_batch_size: int = Field(default=500, ge=1, le=20000)
    write_batch_timeout_s: float = Field(default=0.5, gt=0, le=30)

    @property
    def asyncpg_dsn(self) -> str:
        return str(self.dsn)


class ObservabilitySettings(_Base):
    model_config = SettingsConfigDict(env_prefix="OBS_", extra="ignore")

    log_level: str = Field(default="INFO")
    log_json: bool = Field(default=True)
    metrics_port: int = Field(default=9100, ge=1, le=65535)
    service_name: str = Field(default="racestream")
    environment: str = Field(default="local")


class OpenF1Settings(_Base):
    model_config = SettingsConfigDict(env_prefix="OPENF1_", extra="ignore")

    base_url: str = Field(default="https://api.openf1.org/v1")
    timeout_s: float = Field(default=30.0, gt=0)
    max_retries: int = Field(default=4, ge=0, le=10)
    backoff_base_s: float = Field(default=0.5, gt=0)
    # OpenF1's public tier is rate limited; stay well inside it.
    max_concurrent_requests: int = Field(default=4, ge=1, le=32)
    page_size: int = Field(default=0, ge=0, description="0 = let upstream decide")


class Settings(_Base):
    """Top-level settings aggregate handed to every service."""

    kafka: KafkaSettings = Field(default_factory=KafkaSettings)
    db: DatabaseSettings = Field(default_factory=DatabaseSettings)
    obs: ObservabilitySettings = Field(default_factory=ObservabilitySettings)
    openf1: OpenF1Settings = Field(default_factory=OpenF1Settings)


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()
