"""Pure core of the real-time data-quality service.

:func:`process` turns one raw Kafka message into either a :class:`Clean` event (to be
produced to ``clean-events``) or an :class:`Invalid` one (to be produced to
``dead-letter``). It never raises for bad input and performs no I/O; the only side
effect is staging a user profile in the supplied :class:`ProfileStore`.

Pipeline order: parse -> validate (raw schemas) -> plausibility checks -> apply rules ->
stage profile (``init`` only, using the *transformed* values) -> enrich.
"""

import json
import math
import time
from collections.abc import Callable
from dataclasses import dataclass

from pipeline.dq.enrich import enrich
from pipeline.dq.profiles import Profile, ProfileStore
from pipeline.dq.rules import RuleSet
from pipeline.schemas import SchemaRegistry


@dataclass
class Clean:
    """A validated, transformed and enriched event.

    ``key`` is the Kafka message key (the user id as ``str``). Profiles of ``init`` events
    are not part of the result: they are staged in the :class:`ProfileStore`, which the
    service publishes from ``store.staged()``.
    """

    event: dict
    key: str


# Plausibility limits (see :func:`_plausibility_errors`). A schema-valid number can still be
# poison downstream: a far-future ``time`` would advance the Spark event-time watermark
# (kept in the checkpoint) and make every later real event "late"; a huge ``purchase_value``
# overflows the Postgres ``numeric(14,2)`` column so the sink fails and replays forever.
MAX_FUTURE_SKEW_SECONDS = 300
MAX_PURCHASE_VALUE = 10_000

# Upper bounds that keep a dead-letter record far below Kafka's ~1 MB message limit, so
# that ANY input can be dead-lettered. Without them one oversized (or error-heavy)
# message would make the transactional producer fail forever and wedge the DQ loop.
MAX_DEAD_LETTER_RAW_CHARS = 10_000
MAX_DEAD_LETTER_ERRORS = 20
MAX_DEAD_LETTER_ERROR_CHARS = 500


def _clip(text: str, limit: int) -> str:
    """Return ``text`` cut to ``limit`` characters, marking the cut with an ellipsis."""
    return text if len(text) <= limit else text[:limit] + "..."


@dataclass
class Invalid:
    """A message that failed parsing or validation, with the reasons."""

    raw: bytes
    errors: list[str]

    def to_dead_letter(self) -> bytes:
        """Serialise as the bounded dead-letter payload.

        Keys: ``errors`` (at most ``MAX_DEAD_LETTER_ERRORS`` entries of at most
        ``MAX_DEAD_LETTER_ERROR_CHARS`` characters, plus a "... and N more" entry when
        entries were dropped; validation messages embed the offending value, so they can
        be huge), ``raw`` (the input decoded as UTF-8 with replacement characters, cut to
        ``MAX_DEAD_LETTER_RAW_CHARS``), ``raw_size_bytes`` (original size, always present)
        and ``truncated`` (``true`` when ``raw`` was cut, always present as a bool).
        Any input, including non-UTF-8 bytes, produces valid JSON of bounded size.
        """
        text = self.raw.decode("utf-8", errors="replace")
        errors = [_clip(e, MAX_DEAD_LETTER_ERROR_CHARS) for e in self.errors[:MAX_DEAD_LETTER_ERRORS]]
        if len(self.errors) > MAX_DEAD_LETTER_ERRORS:
            errors.append(f"... and {len(self.errors) - MAX_DEAD_LETTER_ERRORS} more errors")
        payload = {
            "errors": errors,
            "raw": text[:MAX_DEAD_LETTER_RAW_CHARS],
            "raw_size_bytes": len(self.raw),
            "truncated": len(text) > MAX_DEAD_LETTER_RAW_CHARS,
        }
        return json.dumps(payload, ensure_ascii=False).encode("utf-8")


def _reject_constant(name: str):
    """``json`` hook rejecting ``NaN``/``Infinity``, which are not valid JSON."""
    raise ValueError(f"invalid JSON constant {name}")


def _parse(raw: bytes) -> tuple[object, list[str]]:
    """Decode ``raw`` as UTF-8 JSON; return ``(value, errors)`` with errors empty on success."""
    try:
        text = raw.decode("utf-8")
    except UnicodeDecodeError as exc:
        return None, [f"message is not valid UTF-8: {exc}"]
    try:
        return json.loads(text, parse_constant=_reject_constant), []
    except (ValueError, RecursionError) as exc:
        return None, [f"message is not valid JSON: {exc}"]


def _non_finite_paths(value: object, path: str = "$") -> list[str]:
    """Return the JSON paths of all non-finite floats (``inf``/``-inf``/``nan``) in ``value``."""
    if isinstance(value, float):
        return [] if math.isfinite(value) else [path]
    if isinstance(value, dict):
        return [p for k, v in value.items() for p in _non_finite_paths(v, f"{path}.{k}")]
    if isinstance(value, list):
        return [p for i, v in enumerate(value) for p in _non_finite_paths(v, f"{path}[{i}]")]
    return []


def _plausibility_errors(event: dict, now: float) -> list[str]:
    """Return the reasons why a schema-valid ``event`` is still implausible (empty if fine).

    Checks: no non-finite number anywhere (``1e400`` parses to ``inf``); ``time`` not more
    than ``MAX_FUTURE_SKEW_SECONDS`` ahead of ``now`` (catches millisecond timestamps and
    badly skewed clocks; there is deliberately no lower bound, old events are harmless);
    ``purchase_value`` at most ``MAX_PURCHASE_VALUE``.
    """
    bad_paths = _non_finite_paths(event)
    if bad_paths:
        return [f"non-finite number is not allowed at {', '.join(bad_paths)}; values must be finite"]
    errors = []
    if event["time"] > now + MAX_FUTURE_SKEW_SECONDS:
        errors.append(
            f"time {event['time']} is more than {MAX_FUTURE_SKEW_SECONDS}s in the future "
            "(milliseconds instead of seconds, or a skewed client clock?)"
        )
    value = event.get("purchase_value")
    if value is not None and value > MAX_PURCHASE_VALUE:
        errors.append(f"purchase_value {value} exceeds the maximum plausible value {MAX_PURCHASE_VALUE}")
    return errors


def process(
    raw: bytes,
    registry: SchemaRegistry,
    rules: RuleSet,
    store: ProfileStore,
    now: Callable[[], float] = time.time,
) -> Clean | Invalid:
    """Process one raw message into a :class:`Clean` or :class:`Invalid` result.

    For ``init`` events the (transformed) country and platform are staged in ``store``
    under ``str(user-id)`` so that later events in the same batch are enriched. ``now`` is
    the clock used for the future-time plausibility check (injectable for tests). A message
    that fails parsing, schema validation or the plausibility checks is ``Invalid`` and
    stages nothing.
    """
    event, errors = _parse(raw)
    if errors:
        return Invalid(raw, errors)

    errors = registry.validate(event)
    if errors:
        return Invalid(raw, errors)

    errors = _plausibility_errors(event, now())
    if errors:
        return Invalid(raw, errors)

    transformed = rules.apply(event)
    if transformed["event-type"] == "init":
        profile = Profile(country=transformed["country"], platform=transformed["platform"])
        store.stage(str(transformed["user-id"]), profile)

    clean = enrich(transformed, store)
    key_field = "user-a" if clean["event-type"] == "match" else "user-id"
    return Clean(event=clean, key=clean[key_field])
