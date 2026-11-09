"""User profile store (user id -> country/platform) with transactional semantics.

The store behaves like a Kafka Streams GlobalKTable: it is rebuilt at startup from a
compacted Kafka topic via :meth:`ProfileStore.load`, and updated in step with the
Kafka producer transaction of the DQ loop:

* ``stage`` records a profile for the batch in flight; ``get`` already sees it, so a
  purchase that follows its ``init`` in the same batch is enriched.
* ``commit`` is called after the Kafka transaction commits (staged -> committed).
* ``abort`` is called when the transaction aborts (staged discarded, committed kept).

User ids are plain strings; callers normalise them with ``str(...)`` beforehand.
"""

import json
from collections.abc import Iterable
from dataclasses import dataclass


@dataclass(frozen=True)
class Profile:
    """Country and platform of a user, taken from their ``init`` event."""

    country: str
    platform: str

    def to_json(self) -> bytes:
        """Serialise to compact UTF-8 JSON bytes (the compacted-topic value)."""
        payload = {"country": self.country, "platform": self.platform}
        return json.dumps(payload, separators=(",", ":"), ensure_ascii=False).encode("utf-8")

    @classmethod
    def from_json(cls, raw: bytes) -> "Profile":
        """Inverse of :meth:`to_json`."""
        data = json.loads(raw.decode("utf-8"))
        return cls(country=data["country"], platform=data["platform"])


class ProfileStore:
    """In-memory profile map with a staged layer on top of the committed layer."""

    def __init__(self) -> None:
        """Create an empty store."""
        self._committed: dict[str, Profile] = {}
        self._staged: dict[str, Profile] = {}

    def get(self, user_id: str) -> Profile | None:
        """Return the profile for ``user_id`` (staged first, then committed), or ``None``."""
        profile = self._staged.get(user_id)
        if profile is not None:
            return profile
        return self._committed.get(user_id)

    def stage(self, user_id: str, profile: Profile) -> None:
        """Record ``profile`` for the current batch; visible to ``get`` immediately."""
        self._staged[user_id] = profile

    def staged(self) -> dict[str, Profile]:
        """Return a copy of the staged profiles (safe to iterate while producing)."""
        return dict(self._staged)

    def commit(self) -> None:
        """Promote staged profiles to committed and clear the staging layer."""
        self._committed.update(self._staged)
        self._staged.clear()

    def abort(self) -> None:
        """Discard staged profiles; committed profiles are untouched."""
        self._staged.clear()

    def load(self, items: Iterable[tuple[str, Profile]]) -> None:
        """Bulk-load committed profiles (bootstrap from the compacted topic)."""
        self._committed.update(items)
