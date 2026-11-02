import json
import sys
import types
import unittest
from datetime import datetime, timezone
from unittest.mock import patch

from catchuparr.epg_rules import Programme, Window
from catchuparr.schedule import normalize_schedule
from catchuparr.show_windows import (
    channel_rules,
    load_windows,
    plan_channel,
    rules_hash,
    show_window_open,
    store_windows,
)

def load_tasks():
    celery = types.ModuleType("celery")
    celery.shared_task = lambda *, name: lambda function: function
    with patch.dict(sys.modules, {"celery": celery}):
        sys.modules.pop("catchuparr.tasks", None)
        try:
            from catchuparr import tasks
        finally:
            sys.modules.pop("catchuparr.tasks", None)
    return tasks


tasks = load_tasks()
CHANNEL = "00000000-0000-0000-0000-000000000001"
OTHER = "00000000-0000-0000-0000-000000000002"


class FakeRedis:
    def __init__(self):
        self.values = {}

    def set(self, key, value, ex=None):
        self.values[key] = value

    def get(self, key):
        return self.values.get(key)


def show_entry(**overrides):
    entry = {
        "index": 1, "name": "Synthetic", "channel_uuids": [CHANNEL],
        "match": {"title": {"starts_with": "Show"}},
        "padding": {"before": 1.0, "after": 2.0}, "schedule": None, "line": 3,
    }
    entry.update(overrides)
    return entry


def active_with(*entries, timezone_name="UTC"):
    return {"show_rules": list(entries), "recording_schedule": {"timezone": timezone_name, "channels": {}}}


class ShowWindowTests(unittest.TestCase):
    def test_plan_stores_windows_per_rules_hash_and_keeps_running_windows(self):
        active = active_with(show_entry())
        rules = channel_rules(active)[CHANNEL]
        digest = rules_hash(active["show_rules"])
        redis = FakeRedis()
        planned = plan_channel(redis, CHANNEL, digest, rules, [Programme(1000, 2000, "Show A")], 900)
        self.assertEqual([Window(940, 2120, (1,))], planned)
        self.assertEqual(planned, load_windows(redis, CHANNEL, digest))
        self.assertEqual([], load_windows(redis, CHANNEL, "other"))
        # The EPG row vanishes while the show is recording.
        kept = plan_channel(redis, CHANNEL, digest, rules, [], 1500)
        self.assertEqual([Window(940, 2120, (1,))], kept)
        self.assertEqual([], plan_channel(redis, CHANNEL, digest, rules, [], 2200))

    def test_open_window_admits_only_its_channel_and_current_rules(self):
        active = active_with(show_entry())
        redis = FakeRedis()
        digest = rules_hash(active["show_rules"])
        store_windows(redis, CHANNEL, digest, [Window(1000, 2000)])
        self.assertTrue(show_window_open(active, CHANNEL, 1500, redis))
        self.assertTrue(show_window_open(active, CHANNEL, 970, redis))
        self.assertFalse(show_window_open(active, CHANNEL, 2000, redis))
        self.assertFalse(show_window_open(active, OTHER, 1500, redis))
        changed = active_with(show_entry(padding={"before": 0.0, "after": 0.0}))
        self.assertFalse(show_window_open(changed, CHANNEL, 1500, redis))
        self.assertFalse(show_window_open({}, CHANNEL, 1500, redis))
        redis.values[f"catchuparr:showwindows:{CHANNEL}"] = "not json"
        self.assertFalse(show_window_open(active, CHANNEL, 1500, redis))

    def test_schedule_restricts_which_programmes_count(self):
        schedule = normalize_schedule({"daily": [{"start": "18:00", "end": "23:00"}]}).to_snapshot()
        rules = channel_rules(active_with(show_entry(schedule=schedule)))[CHANNEL]
        evening = datetime(2026, 11, 2, 19, 0, tzinfo=timezone.utc).timestamp()
        morning = datetime(2026, 11, 2, 9, 0, tzinfo=timezone.utc).timestamp()
        planned = plan_channel(
            FakeRedis(), CHANNEL, "x", rules,
            [Programme(morning, morning + 1800, "Show 1"), Programme(evening, evening + 1800, "Show 2")],
            morning - 10,
        )
        self.assertEqual(1, len(planned))
        self.assertEqual(evening - 60, planned[0].start)

    def test_window_open_combines_weekly_schedule_and_shows(self):
        redis = FakeRedis()
        active = active_with(show_entry())
        store_windows(redis, CHANNEL, rules_hash(active["show_rules"]), [Window(1000, 2000)])
        closed = normalize_schedule({})
        always = normalize_schedule("continuous")
        now = datetime.fromtimestamp(1500, timezone.utc)
        self.assertTrue(tasks._window_open(active, CHANNEL, closed, "UTC", now, redis))
        self.assertFalse(tasks._window_open(active, CHANNEL, closed, "UTC", datetime.fromtimestamp(3000, timezone.utc), redis))
        self.assertTrue(tasks._window_open(active, CHANNEL, always, "UTC", datetime.fromtimestamp(3000, timezone.utc), redis))
        self.assertFalse(tasks._window_open({"recording_schedule": {}}, CHANNEL, closed, "UTC", now, redis))
        json.dumps(redis.values)


if __name__ == "__main__":
    unittest.main()
