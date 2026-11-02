"""Redis-backed recording windows of the active shows rules."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from datetime import datetime, timezone
from typing import Any

from .epg_rules import (
    Programme,
    ShowRule,
    Window,
    compile_match,
    compile_padding,
    freeze_running,
    plan_windows,
    window_active,
)

WINDOW_KEY = "catchuparr:showwindows:{channel}"
WINDOW_TTL_SECONDS = 300
PLAN_PAST_SECONDS = 3600.0
PLAN_AHEAD_SECONDS = 48 * 3600.0


def rules_hash(show_rules) -> str:
    serialized = json.dumps(show_rules, sort_keys=True, separators=(",", ":"), ensure_ascii=True)
    return hashlib.sha256(serialized.encode("utf-8")).hexdigest()[:16]


def channel_rules(active: Mapping[str, Any]) -> dict[str, list[ShowRule]]:
    """Compile the snapshot's shows rules, grouped by channel."""
    from .schedule import schedule_from_snapshot, schedule_is_active

    timezone_name = (active.get("recording_schedule") or {}).get("timezone")
    result: dict[str, list[ShowRule]] = {}
    for entry in active.get("show_rules") or ():
        before, after = compile_padding({
            "before": int(entry["padding"]["before"]), "after": int(entry["padding"]["after"]),
        })
        restriction = None
        if entry["schedule"] is not None:
            schedule = schedule_from_snapshot(entry["schedule"])
            restriction = (
                lambda start, schedule=schedule: schedule_is_active(
                    schedule, timezone_name, datetime.fromtimestamp(start, timezone.utc)
                )
            )
        rule = ShowRule(entry["index"], compile_match(entry["match"]), before, after, restriction)
        for channel in entry["channel_uuids"]:
            result.setdefault(channel, []).append(rule)
    return result


def store_windows(redis, channel: str, digest: str, windows: list[Window]) -> None:
    payload = {"rules": digest, "windows": [[w.start, w.end, list(w.rules)] for w in windows]}
    redis.set(WINDOW_KEY.format(channel=channel), json.dumps(payload), ex=WINDOW_TTL_SECONDS)


def load_windows(redis, channel: str, digest: str) -> list[Window]:
    """Windows stored for the same shows rules; anything else counts as none."""
    raw = redis.get(WINDOW_KEY.format(channel=channel))
    if not raw:
        return []
    try:
        payload = json.loads(raw if isinstance(raw, str) else raw.decode("utf-8"))
        if payload["rules"] != digest:
            return []
        return [Window(float(s), float(e), tuple(int(r) for r in rules)) for s, e, rules in payload["windows"]]
    except (ValueError, TypeError, KeyError, UnicodeDecodeError):
        return []


def show_window_open(active: Mapping[str, Any], channel: str, now: float, redis=None) -> bool:
    """True while a stored window of this channel is open (or about to open)."""
    rules = active.get("show_rules")
    if not rules or not any(channel in entry["channel_uuids"] for entry in rules):
        return False
    if redis is None:
        from core.utils import RedisClient

        redis = RedisClient.get_client()
    try:
        return window_active(load_windows(redis, channel, rules_hash(rules)), now)
    except Exception:
        return False


def plan_channel(redis, channel: str, digest: str, rules: list[ShowRule],
                 programmes: list[Programme], now: float) -> list[Window]:
    """Plan, keep running windows alive across EPG refreshes and store the result."""
    planned = plan_windows(programmes, rules)
    windows = freeze_running(load_windows(redis, channel, digest), planned, now)
    store_windows(redis, channel, digest, windows)
    return windows
