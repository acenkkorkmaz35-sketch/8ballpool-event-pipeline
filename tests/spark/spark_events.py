"""Builders for clean-event dicts shared by the Spark tests (no pyspark import)."""

from typing import Any

# 2023-11-14 22:14:00 UTC, aligned to a minute boundary.
T0 = 1700000040


def init(user: str, country: str = "TR", platform: str = "ANDROID", t: int = T0 + 5) -> dict[str, Any]:
    """A clean ``init`` event."""
    return {"event-type": "init", "time": t, "user-id": user, "country": country, "platform": platform}


def purchase(user: str, value: float, country: str = "TR", t: int = T0 + 5) -> dict[str, Any]:
    """A clean ``in-app-purchase`` event."""
    return {
        "event-type": "in-app-purchase",
        "time": t,
        "purchase_value": value,
        "user-id": user,
        "product-id": "coins-100",
        "country": country,
        "platform": "IOS",
    }


def match(
    a: str,
    b: str,
    ca: str,
    cb: str,
    pa: str = "ANDROID",
    pb: str = "IOS",
    t: int = T0 + 20,
    user_b_info: bool = True,
) -> dict[str, Any]:
    """A clean ``match`` event with the DQ-enriched ``user-a-platform``/``user-b-platform``.

    The post-match infos carry a decoy platform so a reader that wrongly uses them instead
    of the enriched fields is caught; ``user_b_info=False`` omits ``user-b-postmatch-info``.
    """
    event: dict[str, Any] = {
        "event-type": "match",
        "time": t,
        "user-a": a,
        "user-b": b,
        "user-a-postmatch-info": {
            "coin-balance-after-match": 1,
            "level-after-match": 2,
            "device": "d",
            "platform": "DECOY",
        },
        "winner": a,
        "game-tier": 1,
        "duration": 60,
        "user-a-country": ca,
        "user-b-country": cb,
        "user-a-platform": pa,
        "user-b-platform": pb,
    }
    if user_b_info:
        event["user-b-postmatch-info"] = {
            "coin-balance-after-match": 1,
            "level-after-match": 2,
            "device": "d",
            "platform": "DECOY",
        }
    return event
