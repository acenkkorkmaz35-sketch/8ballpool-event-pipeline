"""Enrich purchase and match events with country/platform from the profile store.

Purchase and match events carry no country, but per-minute revenue/matches by country
need one. Match participants additionally get a platform: the post-match info platform when
present, else the profile platform. Users missing from the store are labelled ``"UNKNOWN"``.
"""

import copy
from typing import Any

from pipeline.constants import UNKNOWN
from pipeline.dq.profiles import Profile, ProfileStore

_USER_FIELDS = ("user-id", "user-a", "user-b")


def _profile_of(store: ProfileStore, user_id: Any) -> Profile | None:
    """Return the profile of ``user_id`` (normalised with ``str``), or None if absent or missing."""
    return store.get(str(user_id)) if user_id is not None else None


def _country_of(store: ProfileStore, user_id: Any) -> str:
    """Return the country of ``user_id`` or ``UNKNOWN``."""
    profile = _profile_of(store, user_id)
    return profile.country if profile else UNKNOWN


def _match_platform_of(store: ProfileStore, event: dict, side: str) -> str:
    """Return the platform of match participant ``side`` (``"a"`` or ``"b"``).

    Order: ``user-<side>-postmatch-info.platform`` when present and non-empty (rules have
    already uppercased it), else the participant's profile platform, else ``UNKNOWN``.
    """
    info = event.get(f"user-{side}-postmatch-info")
    platform = info.get("platform") if isinstance(info, dict) else None
    if isinstance(platform, str) and platform:
        return platform
    profile = _profile_of(store, event.get(f"user-{side}"))
    return profile.platform if profile else UNKNOWN


def enrich(event: dict, store: ProfileStore) -> dict:
    """Return a deep copy of ``event`` with user ids normalised and geo fields added.

    ``user-id``, ``user-a`` and ``user-b`` are converted to ``str`` when present.
    ``in-app-purchase`` gains ``country`` and ``platform``; ``match`` gains
    ``user-a-country``, ``user-b-country``, ``user-a-platform`` and ``user-b-platform``
    (post-match info platform, else profile platform, else ``UNKNOWN``); other event
    types (including ``init``) are only normalised. The input event is never mutated.
    """
    out = copy.deepcopy(event)
    for field in _USER_FIELDS:
        if out.get(field) is not None:
            out[field] = str(out[field])

    event_type = out.get("event-type")
    if event_type == "in-app-purchase":
        profile = _profile_of(store, out.get("user-id"))
        out["country"] = profile.country if profile else UNKNOWN
        out["platform"] = profile.platform if profile else UNKNOWN
    elif event_type == "match":
        out["user-a-country"] = _country_of(store, out.get("user-a"))
        out["user-b-country"] = _country_of(store, out.get("user-b"))
        out["user-a-platform"] = _match_platform_of(store, out, "a")
        out["user-b-platform"] = _match_platform_of(store, out, "b")
    return out
