"""Durable recording windows of the active shows rules.

The planner writes every channel's windows to one JSON file next to the
archive. Redis is not persistent on every install, and a recording must not
stop because Redis restarted or the planner missed a few runs: each window
carries its own end, so a stale file only lets a planned show finish.
"""

from __future__ import annotations

import json
import os
from collections.abc import Mapping
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .epg_rules import (
    Programme,
    ShowRule,
    Window,
    compile_match,
    compile_padding,
    freeze_running,
    plan_windows,
    preview_matches,
    window_active,
)

WINDOWS_FILE = ".catchuparr-show-windows.json"
PLAN_PAST_SECONDS = 3600.0
PLAN_AHEAD_SECONDS = 48 * 3600.0
PLAN_LOCK_KEY = "catchuparr:show-windows:plan"
PLAN_LOCK_SECONDS = 120

_cache: dict[str, tuple[tuple[int, int, int], dict[str, list[Window]]]] = {}


def windows_path(active: Mapping[str, Any]) -> Path:
    return Path(active["archive_root"]).expanduser() / WINDOWS_FILE


def covered_channels(active: Mapping[str, Any]) -> set[str]:
    return {channel for entry in active.get("show_rules") or () for channel in entry["channel_uuids"]}


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
            def restriction(start, schedule=schedule):
                return schedule_is_active(schedule, timezone_name, datetime.fromtimestamp(start, timezone.utc))

        rule = ShowRule(entry["index"], compile_match(entry["match"]), before, after, restriction)
        for channel in entry["channel_uuids"]:
            result.setdefault(channel, []).append(rule)
    return result


def load_windows(path: Path) -> dict[str, list[Window]]:
    """All stored windows by channel; a missing or damaged file counts as none."""
    try:
        stat = path.stat()
        mtime = (stat.st_mtime_ns, stat.st_size, stat.st_ino)
    except OSError:
        return {}
    cached = _cache.get(str(path))
    if cached is not None and cached[0] == mtime:
        return cached[1]
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
        windows = {
            str(channel): [Window(float(s), float(e), tuple(int(r) for r in rules)) for s, e, rules in items]
            for channel, items in payload["channels"].items()
        }
    except (OSError, ValueError, TypeError, KeyError, AttributeError):
        return {}
    _cache[str(path)] = (mtime, windows)
    return windows


def store_windows(path: Path, windows: Mapping[str, list[Window]]) -> None:
    payload = {"channels": {
        channel: [[w.start, w.end, list(w.rules)] for w in items] for channel, items in sorted(windows.items())
    }}
    temporary = path.with_name(f"{path.name}.{os.getpid()}.tmp")
    temporary.write_text(json.dumps(payload, separators=(",", ":")), encoding="utf-8")
    os.replace(temporary, path)


def show_window_open(active: Mapping[str, Any], channel: str, now: float) -> bool:
    """True while a stored window of a channel the shows rules cover is open."""
    if channel not in covered_channels(active):
        return False
    try:
        return window_active(load_windows(windows_path(active)).get(channel, ()), now)
    except Exception:
        return False


def recording_window_open(active: Mapping[str, Any], channel: str, schedule, timezone_name: str,
                          now: datetime) -> bool:
    """Weekly schedule or an open shows window; the one gate for every caller."""
    from .schedule import schedule_is_active

    if schedule_is_active(schedule, timezone_name, now):
        return True
    return bool(active.get("show_rules")) and show_window_open(active, channel, now.timestamp())


def next_show_start(active: Mapping[str, Any], channel: str, now: float) -> float | None:
    if channel not in covered_channels(active):
        return None
    starts = [w.start for w in load_windows(windows_path(active)).get(channel, ()) if w.start > now]
    return min(starts) if starts else None


def load_programmes(active: Mapping[str, Any], now: float) -> dict[str, list[Programme] | None]:
    """EPG programmes around ``now`` for every covered channel, from one query.

    A channel without EPG gets an empty list; ``None`` for all channels means
    the EPG could not be read, so callers keep what they had.
    """
    from datetime import timedelta

    from apps.channels.managers import with_effective_values
    from apps.channels.models import Channel
    from apps.epg.models import ProgramData

    from .logging_utils import error

    covered = covered_channels(active)
    programmes: dict[str, list[Programme] | None] = {channel: [] for channel in covered}
    try:
        channels = with_effective_values(
            Channel.objects.filter(uuid__in=sorted(covered)), select_related_fks=True,
        ).select_related("epg_data__epg_source", "override__epg_data__epg_source")
        channels_by_epg: dict[int, list[str]] = {}
        for channel in channels:
            epg_data = channel.effective_epg_data_obj
            if epg_data is not None:
                channels_by_epg.setdefault(epg_data.id, []).append(str(channel.uuid))
        moment = datetime.fromtimestamp(now, timezone.utc)
        rows = ProgramData.objects.filter(
            epg_id__in=list(channels_by_epg),
            end_time__gt=moment - timedelta(seconds=PLAN_PAST_SECONDS),
            start_time__lt=moment + timedelta(seconds=PLAN_AHEAD_SECONDS),
        ).order_by("start_time")
        for row in rows.iterator(chunk_size=1000):
            programme = Programme(
                row.start_time.timestamp(), row.end_time.timestamp(), row.title or "",
                getattr(row, "sub_title", "") or "", row.description or "",
            )
            for channel_uuid in channels_by_epg.get(row.epg_id, ()):
                programmes[channel_uuid].append(programme)
    except Exception as exc:
        error("show_windows_epg_failed", exc, callsite="show_windows_epg")
        return {channel: None for channel in programmes}
    return programmes


def preview_show_matches(active: Mapping[str, Any], programmes_by_channel: Mapping[str, list[Programme] | None],
                         now: float, *, limit: int = 20) -> dict[str, Any]:
    """Upcoming matches per channel for the validation preview."""
    names = {entry["index"]: entry["name"] for entry in active.get("show_rules") or ()}
    channels: dict[str, list[dict]] = {}
    unreadable = False
    for channel, rules in channel_rules(active).items():
        programmes = programmes_by_channel.get(channel, [])
        if programmes is None:
            unreadable = True
            continue
        matches = preview_matches(programmes, rules, now=now, limit=limit)
        if matches:
            channels[channel] = [{**m, "rules": [names[i] for i in m["rules"]]} for m in matches]
    return {"channels": channels, "epg_unavailable": unreadable}


def plan_all(active: Mapping[str, Any], programmes_by_channel: Mapping[str, list[Programme] | None],
             now: float) -> dict[str, list[Window]]:
    """Plan every covered channel and keep running windows alive.

    ``None`` for a channel means its EPG could not be read this time; its
    previous windows stay unchanged.
    """
    path = windows_path(active)
    previous = load_windows(path)
    rules = channel_rules(active)
    result: dict[str, list[Window]] = {}
    for channel, channel_rule_list in rules.items():
        programmes = programmes_by_channel.get(channel, [])
        if programmes is None:
            result[channel] = [w for w in previous.get(channel, ()) if w.end > now]
            continue
        planned = plan_windows(programmes, channel_rule_list)
        result[channel] = freeze_running(previous.get(channel, ()), planned, now)
    store_windows(path, result)
    return result
