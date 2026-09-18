"""Wire format for events.

JSON, not Avro or Protobuf. The tradeoff is recorded in
docs/engineering-decisions.md: we give up a few bytes and some schema-registry
rigour in exchange for messages a human can read straight off the topic with
``rpk topic consume``, which matters a great deal when debugging a pipeline.
Pydantic gives us the validation that a registry would otherwise provide.
"""

from __future__ import annotations

import orjson

from racestream_common.schemas import Event


class DeserializationError(ValueError):
    """Raised for a message that is not a valid event. Routed to the DLQ."""

    def __init__(self, message: str, raw: bytes) -> None:
        super().__init__(message)
        self.raw = raw


def serialize_event(event: Event) -> bytes:
    return orjson.dumps(event.model_dump(mode="json"))


def deserialize_event(raw: bytes) -> Event:
    """Parse and validate. Never returns a partially-valid event."""
    try:
        data = orjson.loads(raw)
    except orjson.JSONDecodeError as exc:
        raise DeserializationError(f"malformed JSON: {exc}", raw) from exc
    try:
        return Event.model_validate(data)
    except Exception as exc:  # pydantic ValidationError and friends
        raise DeserializationError(f"schema violation: {exc}", raw) from exc
