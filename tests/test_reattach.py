import importlib
import logging
import tempfile
import threading
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch

from tests.test_tasks import (
    FakeRedis,
    _active_configuration,
    _isolated_plugin_modules,
    _recording_task_fake_modules,
)

BEST = {"id": "44", "account_id": "12"}
FALLBACK = {"id": "45", "account_id": "13"}
KEY = "catchuparr:reattach:channel-1"


def _run_recorder(test, outcomes, *, request_during=None):
    """Run a non-sharing recorder through scripted inputs.

    ``outcomes`` maps each input attempt to a function ``(kwargs, ctx)`` that
    returns its result. ``ctx["supervise"]()`` runs one full supervisor pass.
    """
    with tempfile.TemporaryDirectory() as temp:
        redis = FakeRedis()
        active = _active_configuration()
        config = SimpleNamespace(archive_root=Path(temp) / "archive")
        ready = {"status": "ready", "active": active, "config": config}
        attempts = []
        calls = []
        monitors = []
        actual_event = threading.Event()

        class StopEvent:
            supervisor_ticks = 0

            def is_set(self):
                return actual_event.is_set()

            def set(self):
                actual_event.set()

            def wait(self, seconds):
                if seconds == 10:
                    # One supervisor pass per manual call.
                    self.supervisor_ticks += 1
                    return self.supervisor_ticks % 2 == 0
                return actual_event.is_set()

        stop_event = StopEvent()

        class ManualThread:
            ident = None

            def __init__(self, *, target, **_kwargs):
                monitors.append(target)

            def start(self):
                pass

        ctx = {"redis": redis, "supervise": lambda: monitors[0]()}

        class FakeRecorder:
            def __init__(self, *_args, **_kwargs):
                self._mark_next_discontinuity = False

            def run_candidate(self, event, **kwargs):
                calls.append({
                    "stream_id": attempts[-1].stream_id,
                    "startup_timeout": kwargs["startup_timeout"],
                    "discontinuity": self._mark_next_discontinuity,
                    "switch": kwargs.get("attempt_switch"),
                })
                outcome = outcomes[len(calls) - 1](kwargs, ctx)
                if outcome.status == "stopped":
                    event.set()
                return outcome

        def issue(_redis, _lease, **kwargs):
            attempt = SimpleNamespace(
                worker_id="catchuparr-r" + kwargs["candidate"]["id"].zfill(40),
                stream_id=kwargs["candidate"]["id"],
                account_id=kwargs["candidate"]["account_id"],
                input_url="http://dispatcharr.invalid/synthetic-input",
                input_headers={}, revoke=Mock(return_value=True),
                renew=Mock(return_value=True),
            )
            attempts.append(attempt)
            return attempt

        with _isolated_plugin_modules(_recording_task_fake_modules(redis)):
            tasks = importlib.import_module("catchuparr.tasks")
            with (
                patch.object(tasks, "_recorder_admission_state", return_value=ready),
                patch.object(tasks, "_supervisor_recorder_admission_state", return_value=ready),
                patch.object(tasks, "_issue_recorder_capability", return_value=(None, None, None)),
                patch.object(tasks, "RECORDER_CANDIDATE_RETRY_INITIAL_SECONDS", 0.0),
                patch.object(tasks, "threading", SimpleNamespace(
                    Event=lambda: stop_event, RLock=threading.RLock, Thread=ManualThread,
                )),
                patch("catchuparr.configuration.reset_legacy_configuration", return_value=False),
                patch("catchuparr.recorder_proxy.ranked_source_candidates",
                      return_value=[dict(BEST), dict(FALLBACK)]),
                patch("catchuparr.recorder_proxy.candidate_is_current", return_value=True),
                patch("catchuparr.recorder_proxy.issue_recorder_attempt", side_effect=issue),
                patch("catchuparr.recorder_proxy.stop_recorder_attempt", return_value=True),
                patch("catchuparr.engine.recorder.FFmpegCopyRecorder", FakeRecorder),
            ):
                logger = logging.getLogger("catchuparr")
                handler = _ListHandler()
                logger.addHandler(handler)
                previous_level = logger.level
                logger.setLevel(logging.INFO)
                try:
                    result = tasks.record_channel("channel-1", "synthetic-generation", 6)
                finally:
                    logger.setLevel(previous_level)
                    logger.removeHandler(handler)
        return result, calls, [line for line in handler.lines if "reattach" in line], redis


class _ListHandler(logging.Handler):
    def __init__(self):
        super().__init__(logging.INFO)
        self.lines = []

    def emit(self, record):
        self.lines.append(record.getMessage())


def _no_media(_kwargs, _ctx):
    return SimpleNamespace(status="no_media", useful_segments=0)


def _stopped_with_media(kwargs, _ctx):
    kwargs["on_first_media"](0.0)
    return SimpleNamespace(status="stopped", useful_segments=1)


def _fallback_until_reattach(kwargs, ctx):
    """Record on the fallback, then let the button request a reattach."""
    kwargs["on_first_media"](0.0)
    ctx["redis"].set(KEY, "1", ex=300)
    ctx["supervise"]()
    test_switch = kwargs["attempt_switch"]
    assert test_switch.is_set(), "the supervisor did not end the fallback input"
    assert KEY not in ctx["redis"].values, "the request was not consumed"
    return SimpleNamespace(status="switch_requested", useful_segments=3)


class RecorderReattachTests(unittest.TestCase):
    def test_reattach_returns_to_the_preferred_source_with_a_short_startup(self):
        result, calls, logs, _redis = _run_recorder(
            self, [_no_media, _fallback_until_reattach, _stopped_with_media],
        )
        self.assertEqual({"status": "stopped"}, result)
        self.assertEqual(["44", "45", "44"], [call["stream_id"] for call in calls])
        self.assertEqual([60.0, 60.0, 20.0], [call["startup_timeout"] for call in calls])
        self.assertTrue(calls[2]["discontinuity"])
        # Every non-sharing input still gets a switch for a later request.
        self.assertTrue(all(call["switch"] is not None for call in calls))
        self.assertEqual(
            ["[Catchuparr] recorder_reattach_started rank=1",
             "[Catchuparr] recorder_reattached rank=0"],
            logs,
        )

    def test_failed_reattach_falls_back_to_the_next_ranked_source(self):
        result, calls, logs, _redis = _run_recorder(
            self, [_no_media, _fallback_until_reattach, _no_media, _stopped_with_media],
        )
        self.assertEqual({"status": "stopped"}, result)
        self.assertEqual(["44", "45", "44", "45"], [call["stream_id"] for call in calls])
        self.assertEqual([60.0, 60.0, 20.0, 60.0], [call["startup_timeout"] for call in calls])
        self.assertEqual(
            ["[Catchuparr] recorder_reattach_started rank=1",
             "[Catchuparr] recorder_reattach_failed rank=0"],
            logs,
        )

    def test_recorder_on_the_preferred_source_ignores_a_request(self):
        def on_best(kwargs, ctx):
            kwargs["on_first_media"](0.0)
            ctx["redis"].set(KEY, "1", ex=300)
            ctx["supervise"]()
            self.assertFalse(kwargs["attempt_switch"].is_set())
            self.assertNotIn(KEY, ctx["redis"].values)
            return SimpleNamespace(status="stopped", useful_segments=1)

        result, calls, logs, _redis = _run_recorder(self, [on_best])
        self.assertEqual({"status": "stopped"}, result)
        self.assertEqual(["44"], [call["stream_id"] for call in calls])
        self.assertEqual([], logs)

    def test_supervisor_without_a_request_keeps_the_fallback(self):
        def fallback(kwargs, ctx):
            kwargs["on_first_media"](0.0)
            ctx["supervise"]()
            self.assertFalse(kwargs["attempt_switch"].is_set())
            return SimpleNamespace(status="stopped", useful_segments=1)

        result, calls, logs, _redis = _run_recorder(self, [_no_media, fallback])
        self.assertEqual(["44", "45"], [call["stream_id"] for call in calls])
        self.assertEqual([], logs)


class _Control:
    def __init__(self, paused=False):
        self.paused = paused


class RequestReattachTests(unittest.TestCase):
    def _request(self, diagnostics, *, paused=False, preferred=None):
        from catchuparr import reattach

        redis = FakeRedis()
        preferred = preferred or {}
        with (
            patch("catchuparr.recorder_diagnostics.read_recorder_diagnostics",
                  return_value=diagnostics),
            patch("catchuparr.recorder_proxy.is_preferred_source",
                  side_effect=lambda channel, *_args: preferred.get(channel)),
        ):
            result = reattach.request_reattach({"channel_uuids": ""}, _Control(paused), redis_client=redis)
        return result, redis

    def test_marks_only_dedicated_recorders_off_their_preferred_source(self):
        diagnostics = {
            "channel-a": {"phase": "recording", "source": "dedicated", "stream_id": "45", "account_id": "13"},
            "channel-b": {"phase": "recording", "source": "dedicated", "stream_id": "44", "account_id": "12"},
            "channel-c": {"phase": "recording", "source": "borrowed", "stream_id": "46", "account_id": "14"},
            "channel-d": {"phase": "error", "source": "dedicated", "stream_id": "45", "account_id": "13"},
            "channel-e": {"phase": "starting", "source": "dedicated", "stream_id": "47", "account_id": "15"},
        }
        result, redis = self._request(
            diagnostics,
            preferred={"channel-a": False, "channel-b": True, "channel-d": False, "channel-e": None},
        )
        self.assertEqual(["catchuparr:reattach:channel-a"], sorted(redis.values))
        self.assertEqual((1, 4), (result["requested"], result["running"]))
        self.assertEqual("Reattach requested for 1 of 4 running recorders.", result["message"])

    def test_paused_recording_requests_nothing(self):
        diagnostics = {
            "channel-a": {"phase": "recording", "source": "dedicated", "stream_id": "45", "account_id": "13"},
        }
        result, redis = self._request(diagnostics, paused=True, preferred={"channel-a": False})
        self.assertEqual("paused", result["status"])
        self.assertEqual({}, redis.values)

    def test_message_when_every_recorder_is_on_its_best_source(self):
        diagnostics = {
            "channel-b": {"phase": "recording", "source": "dedicated", "stream_id": "44", "account_id": "12"},
        }
        result, redis = self._request(diagnostics, preferred={"channel-b": True})
        self.assertEqual(0, result["requested"])
        self.assertIn("already use their preferred source", result["message"])
        self.assertEqual({}, redis.values)

    def test_unavailable_state_requests_nothing(self):
        from catchuparr import reattach

        result = reattach.request_reattach(None, None, redis_client=FakeRedis())
        self.assertEqual("unavailable", result["status"])


class PreferredSourceTests(unittest.TestCase):
    def test_compares_with_the_first_ranked_source(self):
        from catchuparr import recorder_proxy

        active = {"connection_sharing": {"channel-1": {"enabled": True}}}
        seen = {}

        def ranked(_channel, _active, *, all_sources):
            seen["all_sources"] = all_sources
            return [dict(BEST), dict(FALLBACK)]

        with patch.object(recorder_proxy, "ranked_source_candidates", side_effect=ranked):
            self.assertTrue(recorder_proxy.is_preferred_source("channel-1", "44", "12", active))
            self.assertFalse(recorder_proxy.is_preferred_source("channel-1", "45", "13", active))
            self.assertFalse(recorder_proxy.is_preferred_source("channel-1", "44", "13", active))
        self.assertTrue(seen["all_sources"])

    def test_default_route_has_no_preferred_source(self):
        from catchuparr import recorder_proxy

        with patch.object(recorder_proxy, "ranked_source_candidates", return_value=None):
            self.assertIsNone(recorder_proxy.is_preferred_source("channel-1", "44", "12", {}))


if __name__ == "__main__":
    unittest.main()
