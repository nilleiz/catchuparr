"""Ask recorders on a fallback source to return to their preferred source."""

from __future__ import annotations

REATTACH_REQUEST_KEY = "catchuparr:reattach:{channel}"
# A request that no recorder consumes (for example after a restart) expires.
REATTACH_REQUEST_TTL_SECONDS = 300
_RUNNING_PHASES = frozenset({"starting", "recording"})


def reattach_key(channel_uuid) -> str:
    return REATTACH_REQUEST_KEY.format(channel=channel_uuid)


def request_reattach(settings, control_state, *, redis_client=None, now=None) -> dict:
    """Mark every running dedicated recorder that is not on its best source.

    Borrowed recorders stay untouched. The recorder itself checks again before
    it switches, so a stale mark cannot move a recorder off its best source.
    """
    if settings is None or control_state is None:
        return {"status": "unavailable", "requested": 0, "running": 0,
                "message": "Applied Catchuparr configuration is unavailable."}
    if control_state.paused:
        return {"status": "paused", "requested": 0, "running": 0,
                "message": "Recording is paused; no recorder was asked to reattach."}
    if redis_client is None:
        from core.utils import RedisClient

        redis_client = RedisClient.get_client()
    from .logging_utils import event
    from .recorder_diagnostics import read_recorder_diagnostics
    from .recorder_proxy import is_preferred_source

    diagnostics = read_recorder_diagnostics(redis_client, settings, control_state, now=now)
    running = 0
    requested = 0
    for channel_uuid, diagnostic in sorted(diagnostics.items()):
        if diagnostic.get("phase") not in _RUNNING_PHASES:
            continue
        running += 1
        if diagnostic.get("source") != "dedicated":
            continue
        try:
            preferred = is_preferred_source(
                channel_uuid, diagnostic.get("stream_id"), diagnostic.get("account_id"), settings,
            )
        except Exception:
            continue
        if preferred is False:
            redis_client.set(reattach_key(channel_uuid), "1", ex=REATTACH_REQUEST_TTL_SECONDS)
            requested += 1
    event("recorder_reattach_requested", count=requested, channel_count=running)
    noun = "recorder" if running == 1 else "recorders"
    if not running:
        message = "No recorder is running."
    elif requested:
        message = f"Reattach requested for {requested} of {running} running {noun}."
    else:
        message = f"All {running} running {noun} already use their preferred source or are attached to a viewer."
    return {"status": "ok", "requested": requested, "running": running, "message": message}
