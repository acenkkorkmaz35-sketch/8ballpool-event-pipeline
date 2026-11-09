"""Deterministic simulator of 8 Ball Pool game-server events.

:class:`UserSimulator` produces a stream of ``init``, ``match`` and
``in-app-purchase`` events as ``(key, value)`` pairs ready to be produced to
Kafka. The output is deliberately messy so that it exercises the data-quality
pipeline: mixed-case platforms, ISO country codes, integer vs string user ids,
a missing ``user-b-postmatch-info``, late event times and a small share of
corrupted events that must end up in the dead-letter topic.

All randomness comes from a private :class:`random.Random` seeded in the
constructor, so the same seed and the same ``now`` sequence always produce the
same bytes.
"""

import json
import random

# ISO 3166-1 alpha-2 codes with relative weights (TR, GB and US included).
_COUNTRIES: list[tuple[str, int]] = [
    ("US", 20), ("BR", 10), ("TR", 10), ("GB", 8), ("IN", 8), ("DE", 6),
    ("FR", 6), ("ES", 5), ("MX", 5), ("IT", 4), ("PL", 3), ("SA", 3),
    ("EG", 3), ("ID", 3), ("AU", 2),
]
_PLATFORMS = ["android", "ios", "Android", "iOS"]
_DEVICES = ["Pixel 8", "iPhone 15", "Galaxy S24", "iPhone 13", "Redmi Note 12"]
_PRODUCTS = ["coins-500", "coins-1200", "coins-5000", "cue-pro", "vip-month"]

_FIRST_USER_ID = 100_000
_MAX_LATENESS_SECONDS = 30
_MATCH_SHARE = 0.7
_INT_ID_PROB = 0.5
_MISSING_USER_B_INFO_PROB = 0.1

_CORRUPTIONS = (
    "drop_field",
    "negative_value",
    "string_time",
    "unknown_type",
    "not_json",
)


class UserSimulator:
    """Simulates a population of players and the events they emit.

    Behaviour of :meth:`next_event`:

    * User ids are numeric strings starting at ``"100000"``.
    * A user's first event is always its ``init``. Afterwards roughly 70% of
      events are ``match`` (against a different, already-initialised user) and
      30% are ``in-app-purchase``. While fewer than two users exist a ``match``
      draw falls back to initialising a new user.
    * With probability ``new_user_prob`` (or when no user exists yet) a fresh
      user is initialised, unless the pool already holds ``max_users`` users.
    * In ``init`` and purchase events ``user-id`` is an ``int`` half of the
      time and a ``str`` otherwise; ``match`` always uses strings.
    * With probability ``invalid_rate`` the event is corrupted (a required
      field dropped, a negative value, ``time`` as a string, an unknown
      ``event-type``, or non-JSON bytes).

    Corruption happens *after* the simulator state is updated: if an ``init``
    is corrupted the user is still treated as initialised. This deliberately
    models real data loss, so later matches reference a user the pipeline
    never saw initialised and the UNKNOWN-country path gets exercised.
    """

    def __init__(
        self,
        seed: int,
        max_users: int = 1000,
        invalid_rate: float = 0.01,
        new_user_prob: float = 0.05,
    ):
        """Create a simulator.

        Args:
            seed: Seed of the private random generator (determinism).
            max_users: Cap on the number of distinct users (at least 2).
            invalid_rate: Probability that an emitted event is corrupted.
            new_user_prob: Probability of initialising a fresh user per event.
        """
        if max_users < 2:
            raise ValueError("max_users must be at least 2")
        self._rng = random.Random(seed)
        self._max_users = max_users
        self._invalid_rate = invalid_rate
        self._new_user_prob = new_user_prob
        self._next_id = _FIRST_USER_ID
        # user id -> mutable profile used to keep match results consistent.
        self._users: dict[str, dict] = {}
        self._ids: list[str] = []
        self._country_codes = [c for c, _ in _COUNTRIES]
        self._country_weights = [w for _, w in _COUNTRIES]

    def next_event(self, now: int) -> tuple[str, bytes]:
        """Return the next ``(key, value)`` pair for the producer.

        Args:
            now: Current epoch seconds; the event time lags it by 0-30 s.

        Returns:
            The Kafka key (the user id, ``user-a`` for matches) and the value:
            compact UTF-8 JSON, or deliberately invalid bytes when corrupted.
        """
        time = max(0, now - self._rng.randint(0, _MAX_LATENESS_SECONDS))
        can_add = len(self._ids) < self._max_users
        wants_new = not self._ids or self._rng.random() < self._new_user_prob
        if can_add and wants_new:
            event, key = self._init_event(time)
        elif self._rng.random() < _MATCH_SHARE:
            if len(self._ids) >= 2:
                event, key = self._match_event(time)
            else:  # fewer than two users: a match is impossible, add a user
                event, key = self._init_event(time)
        else:
            event, key = self._purchase_event(time)

        if self._rng.random() < self._invalid_rate:
            return key, self._corrupt(event)
        return key, self._encode(event)

    # -- event builders -------------------------------------------------

    def _wire_id(self, user_id: str) -> str | int:
        """Return ``user_id`` as an int or a str (50/50) for init/IAP events."""
        return int(user_id) if self._rng.random() < _INT_ID_PROB else user_id

    def _init_event(self, time: int) -> tuple[dict, str]:
        """Register a fresh user and build its ``init`` event."""
        user_id = str(self._next_id)
        self._next_id += 1
        profile = {
            "platform": self._rng.choice(_PLATFORMS),
            "device": self._rng.choice(_DEVICES),
            "coins": self._rng.randint(100, 5000),
            "level": self._rng.randint(1, 30),
        }
        self._users[user_id] = profile
        self._ids.append(user_id)
        event = {
            "event-type": "init",
            "time": time,
            "user-id": self._wire_id(user_id),
            "country": self._rng.choices(self._country_codes, self._country_weights)[0],
            "platform": profile["platform"],
        }
        return event, user_id

    def _purchase_event(self, time: int) -> tuple[dict, str]:
        """Build an ``in-app-purchase`` event for a random initialised user."""
        user_id = self._rng.choice(self._ids)
        event = {
            "event-type": "in-app-purchase",
            "time": time,
            "purchase_value": round(self._rng.uniform(0.99, 49.99), 2),
            "user-id": self._wire_id(user_id),
            "product-id": self._rng.choice(_PRODUCTS),
        }
        return event, user_id

    def _match_event(self, time: int) -> tuple[dict, str]:
        """Build a ``match`` between two distinct initialised users."""
        user_a, user_b = self._rng.sample(self._ids, 2)
        winner = self._rng.choice([user_a, user_b])
        event = {
            "event-type": "match",
            "time": time,
            "user-a": user_a,
            "user-b": user_b,
            "user-a-postmatch-info": self._postmatch(user_a, user_a == winner),
            "winner": winner,
            "game-tier": self._rng.randint(1, 5),
            "duration": self._rng.randint(60, 900),
        }
        # Build user-b's info unconditionally so the user's balance evolves the
        # same way whether or not the block is emitted.
        info_b = self._postmatch(user_b, user_b == winner)
        if self._rng.random() >= _MISSING_USER_B_INFO_PROB:
            event["user-b-postmatch-info"] = info_b
        return event, user_a

    def _postmatch(self, user_id: str, won: bool) -> dict:
        """Update a user's coins/level after a match and return the info block."""
        profile = self._users[user_id]
        if won:
            profile["coins"] += self._rng.randint(50, 500)
            if self._rng.random() < 0.3:
                profile["level"] += 1
        else:
            profile["coins"] = max(0, profile["coins"] - self._rng.randint(10, 300))
        return {
            "coin-balance-after-match": profile["coins"],
            "level-after-match": profile["level"],
            "device": profile["device"],
            "platform": profile["platform"],
        }

    # -- serialisation and corruption ----------------------------------

    @staticmethod
    def _encode(event: dict) -> bytes:
        """Serialise an event as compact UTF-8 JSON."""
        return json.dumps(event, separators=(",", ":")).encode("utf-8")

    def _corrupt(self, event: dict) -> bytes:
        """Return ``event`` broken in one randomly chosen, schema-violating way."""
        kind = self._rng.choice(_CORRUPTIONS)
        bad = dict(event)
        if kind == "not_json":
            return b"\xff\xfe<not-json>" + self._encode(event)[: self._rng.randint(1, 12)]
        if kind == "drop_field":
            required = [k for k in bad if k != "user-b-postmatch-info"]
            del bad[self._rng.choice(required)]
        elif kind == "negative_value":
            field = "purchase_value" if "purchase_value" in bad else "time"
            bad[field] = -bad[field] - 1
        elif kind == "string_time":
            bad["time"] = str(bad["time"])
        elif kind == "unknown_type":
            bad["event-type"] = "unknown-event"
        return self._encode(bad)
