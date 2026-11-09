"""JSON Schema registry: validates raw events against the draft-03 task schemas.

The schema for an event is selected by the event's ``event-type`` field. Each
schema file declares its own event type as a single-value ``enum`` on the
``event-type`` property, and that value is used as the registry key.
"""

import json
from pathlib import Path

from jsonschema import Draft3Validator


class SchemaRegistry:
    """Holds one compiled draft-03 validator per event type."""

    def __init__(self, validators: dict[str, Draft3Validator]):
        """Create a registry from a mapping of event type to validator."""
        self._validators = validators

    @classmethod
    def from_dir(cls, path: Path) -> "SchemaRegistry":
        """Load every ``*.json`` schema in ``path`` and key it by its event type."""
        validators: dict[str, Draft3Validator] = {}
        for schema_file in sorted(Path(path).glob("*.json")):
            schema = json.loads(schema_file.read_text(encoding="utf-8"))
            Draft3Validator.check_schema(schema)
            event_type = schema["properties"]["event-type"]["enum"][0]
            validators[event_type] = Draft3Validator(schema)
        return cls(validators)

    def validate(self, event: dict) -> list[str]:
        """Return the validation errors for ``event``; an empty list means valid.

        An unknown or missing ``event-type`` yields exactly one error.
        """
        if not isinstance(event, dict):
            return [f"event must be a JSON object, got {type(event).__name__}"]
        event_type = event.get("event-type")
        validator = self._validators.get(event_type) if isinstance(event_type, str) else None
        if validator is None:
            return [f"unknown or missing event-type: {event_type!r}"]
        errors = sorted(validator.iter_errors(event), key=lambda e: list(e.path))
        return [self._format(e) for e in errors]

    @staticmethod
    def _format(error) -> str:
        """Render a validation error as ``<field path>: <message>``."""
        location = ".".join(str(p) for p in error.path) or "<root>"
        return f"{location}: {error.message}"
