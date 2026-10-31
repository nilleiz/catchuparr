import importlib
import tempfile
import threading
import time
import unittest
from contextlib import ExitStack
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from catchuparr.recorder_proxy import configuration_generation
from tests.test_tasks import (
    FakeRedis,
    _active_configuration,
    _isolated_plugin_modules,
    _recording_task_fake_modules,
)


class _StopEvent:
    def __init__(self):
        self.stopped = False

    def set(self):
        self.stopped = True

    def is_set(self):
        return self.stopped

    def wait(self, _timeout=None):
        return self.stopped


class _InlineThread:
    """Keep the periodic supervisor out of these deterministic wiring tests."""

    def __init__(self, *, target, **_kwargs):
        self.target = target
        self.ident = None

    def start(self):
        self.ident = 1

    def join(self, timeout=None):
        return None

    def is_alive(self):
        return False


class _FakeTail:
    def __init__(self, calls, *, joined=True, staged=None, stop_on=None, stop_event=None):
        self.calls = calls
        self.joined = joined
        self.staged = staged
        self.stop_on = stop_on
        self.stop_event = stop_event

    def _record(self, name, *args):
        self.calls.append((name, *args))
        if name == self.stop_on:
            self.stop_event().set()

    def join(self, outcome, marker):
        self._record("join", outcome, marker)
        return self.joined

    def start(self, *, startup_timeout, media_idle_timeout):
        self._record("start")

    def borrow_cleaned(self):
        self._record("borrow_cleaned")

    def hand_over(self, recorder, switch):
        self._record("hand_over", recorder.kind, switch is not None)
        return self.staged

    def close(self):
        self._record("close")


class _RecordChannelHarness:
    def run_record_channel(self, *, tail=None, recover_tail=True, outcome_return=True,
                           authority=True, issue_errors=(), wait_seconds=90.0):
        """Borrow, return at the deadline, then one dedicated attempt that stops."""
        calls, recorder_runs, prepare_calls, acknowledged = [], [], [], []
        holder = {"issued": [], "reaped": []}
        pending_issue_errors = list(issue_errors)
        stop_box = {"event": _StopEvent()}
        with tempfile.TemporaryDirectory() as temp:
            archive_root = Path(temp) / "archive"
            redis = FakeRedis()
            config = SimpleNamespace(channel_uuids=("channel-1",), archive_root=archive_root)
            active = _active_configuration()
            active["synchronize_before_switch"] = True
            active["connection_sharing"] = {"channel-1": {
                "enabled": True, "live_stability_seconds": 0,
                "last_viewer_return_seconds": 1, "min_switch_interval_seconds": 0,
            }}
            expected_generation = configuration_generation(active)
            opening = SimpleNamespace(
                episode_id="episode-synthetic", capability=SimpleNamespace(intent_epoch=1),
            )
            token = SimpleNamespace(intent_epoch=2, not_before=time.time() - 1)
            state = {"returned": False, "polls": 0}

            class Attempt:
                def __init__(self, candidate):
                    self.worker_id = "catchuparr-r" + candidate["id"].zfill(40)
                    self.stream_id = candidate["id"]
                    self.input_url = "http://dispatcharr/catchuparr/recorder/channel-1"
                    self.input_headers = {"X-Catchuparr-Recorder": "signed-capability"}

                def renew(self, _redis):
                    return True

                def revoke(self, _redis):
                    return True

                def operation_needs_switch(self, _redis):
                    return False

                def acknowledge_reaped(self, _redis):
                    return True

            class Coordinator:
                def __init__(self, *_args, **kwargs):
                    self.dedicated_source_available = kwargs.get("dedicated_source_available")

                def poll(self, **_kwargs):
                    state["polls"] += 1
                    return opening if state["polls"] == 1 else None

                def viewer_return_deadline_reached(self, *, now):
                    return outcome_return

                def record_return_after_cleanup(self, prepared, *, now, outcome):
                    calls.append(("return_after_cleanup", outcome))
                    state["returned"] = True
                    return True

                def pending_return_intent(self):
                    return token if state["returned"] else None

                def recovered_return_receipt(self):
                    return None

                def acknowledge_return_media(self, prepared, tok, attempt, candidate, *, media_at):
                    acknowledged.append((prepared, tok))
                    return True

                def return_ack_retryable(self, *_args, **_kwargs):
                    return False

            class Recorder:
                def __init__(self, _store, _channel, input_url, _work, **_kwargs):
                    self.kind = "borrow" if "/catchuparr/sharing/" in input_url else "dedicated"
                    self._current_attempt_segment_id = "segment-9" if self.kind == "borrow" else None
                    self._mark_next_discontinuity = False

                def run_candidate(self, stop_event, **kwargs):
                    recorder_runs.append((self.kind, kwargs.get("staged_input")))
                    if self.kind == "borrow":
                        return SimpleNamespace(status="exited", useful_segments=1, return_code=0)
                    kwargs["on_first_media"](time.time())
                    stop_event.set()
                    return SimpleNamespace(status="stopped", useful_segments=1, return_code=-15)

                def prepare_synchronized_return(self, marker, stop_event, switch):
                    prepare_calls.append(marker)
                    return None

            def issue(_redis, _lease, **kwargs):
                holder["issued"].append(kwargs.get("sharing_authority"))
                if pending_issue_errors:
                    raise pending_issue_errors.pop(0)
                return Attempt(kwargs["candidate"])

            def reap(work_root, channel):
                holder["reaped"].append((Path(work_root).name, channel))
                return 0

            def start_watch(*args):
                holder["args"] = args
                if tail is None:
                    return None, None, None
                tail.stop_event = lambda: stop_box["event"]
                return tail, threading.Event(), _InlineThread(target=None)

            with _isolated_plugin_modules(_recording_task_fake_modules(redis)):
                tasks = importlib.import_module("catchuparr.tasks")
                with ExitStack() as stack:
                    enter = stack.enter_context
                    enter(patch.object(tasks, "_start_return_tail_watch", side_effect=start_watch))
                    enter(patch.object(tasks, "RECORDER_CLEANUP_RETRY_SECONDS", 0.0))
                    enter(patch.object(tasks, "PRIOR_SHARING_OPERATION_WAIT_SECONDS", wait_seconds))
                    enter(patch.object(tasks, "PRIOR_SHARING_OPERATION_RETRY_SECONDS", 0.0))
                    enter(patch("catchuparr.engine.recorder.reap_orphaned_inputs", side_effect=reap))
                    enter(patch("catchuparr.sharing_tail.recover_stale_tail", return_value=recover_tail))
                    enter(patch("catchuparr.runtime.load_config", return_value=config))
                    enter(patch("catchuparr.configuration.reset_legacy_configuration", return_value=False))
                    enter(patch("catchuparr.configuration.load_applied_state",
                                return_value=(active, SimpleNamespace(paused=False, generation=6))))
                    enter(patch("catchuparr.tasks._recorder_admission_state", return_value={
                        "status": "ready", "active": active, "config": config,
                        "control": SimpleNamespace(generation=6),
                    }))
                    enter(patch("catchuparr.recorder_proxy.ranked_source_candidates",
                                return_value=[{"id": "44", "account_id": "fast"}]))
                    enter(patch("catchuparr.recorder_proxy.current_sharing_authority_context",
                                return_value={"authority_revision": 2, "catalog_generation": "catalog"}
                                if authority else None))
                    enter(patch("catchuparr.recorder_proxy.issue_recorder_attempt", side_effect=issue))
                    enter(patch("catchuparr.recorder_proxy.stop_recorder_attempt", return_value=True))
                    enter(patch("catchuparr.tasks._issue_recorder_capability", return_value=(None, None, None)))
                    enter(patch("catchuparr.sharing_runtime.RecorderBorrowCoordinator", Coordinator))
                    enter(patch("catchuparr.sharing_episode.BorrowEpisodeLedger", return_value=object()))
                    enter(patch("catchuparr.sharing_recovery.recover_superseded_borrow", return_value=True))
                    enter(patch("catchuparr.sharing_runtime.retry_durable_open_cleanups", return_value=(0, 0)))
                    enter(patch("catchuparr.sharing_runtime.retry_pending_open_cleanups"))
                    enter(patch("catchuparr.sharing_runtime.borrow_cleanup_acknowledged", return_value=True))
                    enter(patch("catchuparr.adapters.sharing_borrow_route.encode_borrow_capability",
                                return_value="synthetic-borrow-token"))
                    enter(patch("catchuparr.engine.recorder.FFmpegCopyRecorder", Recorder))
                    enter(patch("catchuparr.adapters.recorder_proxy.core_api_supported", return_value=True))
                    enter(patch("catchuparr.adapters.recorder_proxy.install_proxyserver_cleanup_hook",
                                return_value=True))
                    enter(patch("catchuparr.tasks.threading", SimpleNamespace(
                        Event=lambda: stop_box["event"], Lock=threading.Lock,
                        RLock=threading.RLock, Thread=_InlineThread,
                    )))
                    result = tasks.record_channel("channel-1", expected_generation, 6)
        return result, calls, recorder_runs, prepare_calls, acknowledged, holder


class ReturnTailWiringTests(_RecordChannelHarness, unittest.TestCase):
    def test_joined_tail_hands_the_dedicated_input_over_instead_of_a_packet_return(self):
        calls = []
        closed = []
        staged = SimpleNamespace(name="staged-dedicated", close=lambda: closed.append(True))
        tail = _FakeTail(calls, staged=staged)
        result, events, runs, prepares, acknowledged, holder = self.run_record_channel(tail=tail)
        self.assertEqual({"status": "stopped"}, result)
        self.assertEqual(
            [("join", "return", "segment-9"), ("start",), ("borrow_cleaned",),
             ("hand_over", "dedicated", True)],
            calls,
        )
        self.assertIn(("return_after_cleanup", "return"), events)
        self.assertEqual([("borrow", None), ("dedicated", staged)], runs)
        self.assertEqual([], prepares, "the packet return must not run beside a tail")
        self.assertEqual(1, len(acknowledged))
        self.assertEqual([True], closed, "the staged input is released after its attempt")
        # The watcher is built for the exact borrowed recorder and dedicated candidate.
        self.assertEqual("borrow", holder["args"][3].kind)
        self.assertEqual({"id": "44", "account_id": "fast"}, holder["args"][11]())

    def test_failed_join_closes_the_tail_and_keeps_the_packet_return(self):
        calls = []
        tail = _FakeTail(calls, joined=False)
        result, _events, runs, prepares, acknowledged, _holder = self.run_record_channel(tail=tail)
        self.assertEqual({"status": "stopped"}, result)
        self.assertEqual([("join", "return", "segment-9"), ("close",)], calls)
        self.assertEqual(["segment-9"], prepares)
        self.assertEqual([("borrow", None), ("dedicated", None)], runs)
        self.assertEqual(1, len(acknowledged))

    def test_borrow_that_does_not_return_offers_no_marker_to_the_tail(self):
        calls = []
        tail = _FakeTail(calls, joined=False)
        _result, _events, _runs, _prepares, _ack, _holder = self.run_record_channel(
            tail=tail, outcome_return=False,
        )
        self.assertEqual(("join", "failure", None), calls[0])
        self.assertEqual(("close",), calls[1])

    def test_without_a_tail_the_return_is_unchanged(self):
        result, _events, runs, prepares, acknowledged, _holder = self.run_record_channel(tail=None)
        self.assertEqual({"status": "stopped"}, result)
        self.assertEqual(["segment-9"], prepares)
        self.assertEqual([("borrow", None), ("dedicated", None)], runs)
        self.assertEqual(1, len(acknowledged))

    def test_tail_left_running_at_a_stop_is_closed_on_exit(self):
        calls = []
        tail = _FakeTail(calls, stop_on="borrow_cleaned")
        result, _events, runs, _prepares, acknowledged, _holder = self.run_record_channel(tail=tail)
        self.assertEqual({"status": "stopped"}, result)
        self.assertEqual([("join", "return", "segment-9"), ("start",), ("borrow_cleaned",), ("close",)],
                         calls)
        self.assertEqual([("borrow", None)], runs)
        self.assertEqual([], acknowledged)

    def test_unrecovered_stale_tail_blocks_the_recorder_start(self):
        result, _events, runs, _prepares, _ack, _holder = self.run_record_channel(recover_tail=False)
        self.assertEqual({"status": "sharing_cleanup_pending"}, result)
        self.assertEqual([], runs)


class RecorderStartRecoveryTests(_RecordChannelHarness, unittest.TestCase):
    """Startup recovery shared by every recorder run, exercised through the same harness."""

    def test_orphaned_inputs_are_reaped_after_the_lease_is_held(self):
        _result, _events, _runs, _prepares, _ack, holder = self.run_record_channel()
        self.assertEqual([("work", "channel-1")], holder["reaped"])

    def test_prior_sharing_operation_is_awaited_until_it_closes(self):
        from catchuparr.recorder_proxy import PriorSharingOperationPending

        result, _events, runs, _prepares, acknowledged, holder = self.run_record_channel(
            issue_errors=[PriorSharingOperationPending("synthetic"), PriorSharingOperationPending("synthetic")],
        )
        self.assertEqual({"status": "stopped"}, result)
        self.assertEqual(3, len(holder["issued"]))
        self.assertEqual("dedicated", runs[-1][0])
        self.assertEqual(1, len(acknowledged))

    def test_prior_sharing_operation_that_stays_open_ends_the_run_in_order(self):
        from catchuparr.recorder_proxy import PriorSharingOperationPending

        result, _events, runs, _prepares, _ack, _holder = self.run_record_channel(
            issue_errors=[PriorSharingOperationPending("synthetic")], wait_seconds=0.0,
        )
        self.assertEqual({"status": "sharing_cleanup_pending"}, result)
        self.assertNotIn("dedicated", [kind for kind, _staged in runs])

    def test_stale_sharing_authority_records_from_a_dedicated_source(self):
        result, events, runs, _prepares, _ack, holder = self.run_record_channel(authority=False)
        self.assertEqual({"status": "stopped"}, result)
        self.assertEqual([None], holder["issued"])
        self.assertEqual([("dedicated", None)], runs)
        self.assertNotIn(("return_after_cleanup", "return"), events)


class ReturnTailWatchGateTests(unittest.TestCase):
    def test_watch_starts_only_for_a_configured_delayed_return(self):
        with _isolated_plugin_modules(_recording_task_fake_modules(FakeRedis())):
            tasks = importlib.import_module("catchuparr.tasks")
            self._check_gates(tasks)

    def _check_gates(self, tasks):
        prepared = SimpleNamespace(capability=SimpleNamespace(connection_identity=("c",) * 8))
        base = {"synchronize_before_switch": True, "handover_delayed_account_ids": ["10"],
                "handover_max_overlap_seconds": 90}
        cases = {
            "sync off": ({**base, "synchronize_before_switch": False}, prepared, "digest"),
            "no delayed sources": ({**base, "handover_delayed_account_ids": []}, prepared, "digest"),
            "no overlap": ({**base, "handover_max_overlap_seconds": None}, prepared, "digest"),
            "no stats digest": (base, prepared, None),
            "recovered return": (base, SimpleNamespace(intent_epoch=1), "digest"),
        }
        for name, (active, opening, digest) in cases.items():
            with self.subTest(name):
                self.assertEqual(
                    (None, None, None),
                    tasks._start_return_tail_watch(
                        None, "channel-1", opening, None, None, None, 1, None,
                        digest, active, threading.Event(), lambda: None,
                    ),
                )


if __name__ == "__main__":
    unittest.main()
