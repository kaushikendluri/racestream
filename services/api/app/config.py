"""API-specific settings, layered on the shared ones."""

from __future__ import annotations

from functools import lru_cache

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

from racestream_common.config import Settings, get_settings


class ApiSettings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="API_", env_file=".env", extra="ignore")

    host: str = Field(default="0.0.0.0")
    port: int = Field(default=8000, ge=1, le=65535)
    metrics_port: int = Field(default=9100, ge=1, le=65535)

    # Explicit origins, never "*": the WebSocket and the API are same-origin in
    # production but cross-origin during local development, and a wildcard here
    # would be a habit worth not forming.
    cors_origins: str = Field(default="http://localhost:5173,http://localhost:3000")

    rate_limit_per_minute: int = Field(default=120, ge=1)
    # Chaos endpoints mutate live pipeline state. Off unless explicitly enabled,
    # so a deployed instance does not expose "stop the consumer" to the internet.
    enable_chaos_endpoints: bool = Field(default=True)
    # Prometheus scraping is internal; exposing it publicly leaks topology.
    expose_metrics_endpoint: bool = Field(default=True)

    # A client is considered stale, then disconnected, after these gaps with no
    # event. The frontend mirrors these thresholds so both agree on "LIVE".
    stale_after_s: float = Field(default=5.0, gt=0)
    disconnected_after_s: float = Field(default=15.0, gt=0)

    @field_validator("cors_origins")
    @classmethod
    def _no_wildcard(cls, v: str) -> str:
        if "*" in v:
            raise ValueError(
                "API_CORS_ORIGINS must list explicit origins; '*' is not accepted"
            )
        return v

    @property
    def cors_origin_list(self) -> list[str]:
        return [o.strip() for o in self.cors_origins.split(",") if o.strip()]


@lru_cache(maxsize=1)
def get_api_settings() -> ApiSettings:
    return ApiSettings()


@lru_cache(maxsize=1)
def get_shared_settings() -> Settings:
    return get_settings()
