"""Structured JSON logging.

Every service emits the same shape so that logs from ingestion, the processor
and the API can be correlated on ``session_id`` / ``event_id`` without a
per-service parser.
"""

from __future__ import annotations

import logging
import sys
from typing import Any

import structlog

_CONFIGURED = False

_REDACT_KEYS = {"password", "dsn", "token", "secret", "api_key", "authorization"}


def _redact(_logger: Any, _name: str, event_dict: dict) -> dict:
    """Drop credential-ish values before they reach stdout."""
    for key in list(event_dict):
        if any(marker in key.lower() for marker in _REDACT_KEYS):
            event_dict[key] = "[redacted]"
    return event_dict


def configure_logging(
    service: str, level: str = "INFO", json_output: bool = True, environment: str = "local"
) -> None:
    """Install the shared processor chain. Idempotent."""
    global _CONFIGURED
    if _CONFIGURED:
        return

    renderer = (
        structlog.processors.JSONRenderer()
        if json_output
        else structlog.dev.ConsoleRenderer(colors=False)
    )

    structlog.configure(
        processors=[
            structlog.contextvars.merge_contextvars,
            structlog.stdlib.add_log_level,
            structlog.processors.TimeStamper(fmt="iso", utc=True),
            structlog.processors.StackInfoRenderer(),
            structlog.processors.format_exc_info,
            _redact,
            renderer,
        ],
        wrapper_class=structlog.make_filtering_bound_logger(
            getattr(logging, level.upper(), logging.INFO)
        ),
        logger_factory=structlog.PrintLoggerFactory(file=sys.stdout),
        cache_logger_on_first_use=True,
    )
    structlog.contextvars.bind_contextvars(service=service, environment=environment)

    # Route stdlib loggers (uvicorn, aiokafka, asyncpg) through the same sink.
    logging.basicConfig(
        format="%(message)s", stream=sys.stdout, level=getattr(logging, level.upper(), logging.INFO)
    )
    _CONFIGURED = True


def get_logger(name: str | None = None) -> structlog.stdlib.BoundLogger:
    return structlog.get_logger(name)
