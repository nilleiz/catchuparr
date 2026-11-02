import sys
import tempfile
import types
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

from catchuparr.epg_rules import Programme, Show, Window
from catchuparr.schedule import normalize_schedule
from catchuparr.show_windows import (
    load_windows,
    next_show_start,
    plan_all,
    recording_window_open,
    show_window_open,
    store_windows,
    windows_path,
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


def show_entry(**overrides):
    entry = {
        "index": 1, "name": "Synthetic", "channel_uuids": [CHANNEL],
        "match": {"title": {"starts_with": "Show"}},
        "padding": {"before": 1.0, "after": 2.0}, "schedule": None, "line": 3,
    }
    entry.update(overrides)
    return entry


def bare(by_channel):
    return {
        channel: [Window(w.start, w.end, w.rules) for w in windows] for channel, windows in by_channel.items()
    }


class ShowWindowTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.root = Path(self.directory.name)

    def tearDown(self):
        self.directory.cleanup()

    def active(self, *entries):
        return {
            "archive_root": str(self.root), "show_rules": list(entries),
            "recording_schedule": {"timezone": "UTC", "channels": {}},
        }

    def test_plan_keeps_running_windows_when_the_epg_row_vanishes(self):
        active = self.active(show_entry())
        planned = plan_all(active, {CHANNEL: [Programme(1000, 2000, "Show A")]}, 900)
        self.assertEqual({CHANNEL: [Window(940, 2120, (1,))]}, bare(planned))
        self.assertEqual(planned, load_windows(windows_path(active)))
        self.assertEqual({CHANNEL: [Window(940, 2120, (1,))]}, bare(plan_all(active, {CHANNEL: []}, 1500)))
        self.assertEqual({CHANNEL: []}, plan_all(active, {CHANNEL: []}, 2200))

    def test_unreadable_epg_keeps_previous_windows(self):
        active = self.active(show_entry())
        plan_all(active, {CHANNEL: [Programme(5000, 6000, "Show A")]}, 900)
        self.assertEqual({CHANNEL: [Window(4940, 6120, (1,))]}, bare(plan_all(active, {CHANNEL: None}, 1000)))

    def test_running_window_survives_a_rule_edit_but_not_its_removal(self):
        active = self.active(show_entry())
        plan_all(active, {CHANNEL: [Programme(1000, 2000, "Show A")]}, 900)
        edited = self.active(show_entry(match={"title": {"starts_with": "Other"}}))
        self.assertEqual(
            {CHANNEL: [Window(940, 2120, (1,))]},
            bare(plan_all(edited, {CHANNEL: [Programme(1000, 2000, "Show A")]}, 1500)),
        )
        self.assertTrue(show_window_open(edited, CHANNEL, 1500))
        removed = self.active(show_entry(channel_uuids=[OTHER]))
        self.assertFalse(show_window_open(removed, CHANNEL, 1500))

    def test_window_file_drives_admission(self):
        active = self.active(show_entry())
        store_windows(windows_path(active), {CHANNEL: [Window(1000, 2000), Window(5000, 6000)]})
        self.assertTrue(show_window_open(active, CHANNEL, 1500))
        self.assertTrue(show_window_open(active, CHANNEL, 970))
        self.assertFalse(show_window_open(active, CHANNEL, 2000))
        self.assertFalse(show_window_open(active, OTHER, 1500))
        self.assertFalse(show_window_open({}, CHANNEL, 1500))
        self.assertEqual(5000, next_show_start(active, CHANNEL, 2000))
        windows_path(active).write_text("not json", encoding="utf-8")
        self.assertFalse(show_window_open(active, CHANNEL, 1500))

    def test_schedule_restricts_which_programmes_count(self):
        schedule = normalize_schedule({"daily": [{"start": "18:00", "end": "23:00"}]}).to_snapshot()
        active = self.active(show_entry(schedule=schedule))
        evening = datetime(2026, 11, 2, 19, 0, tzinfo=timezone.utc).timestamp()
        morning = datetime(2026, 11, 2, 9, 0, tzinfo=timezone.utc).timestamp()
        planned = plan_all(active, {CHANNEL: [
            Programme(morning, morning + 1800, "Show 1"), Programme(evening, evening + 1800, "Show 2"),
        ]}, morning - 10)[CHANNEL]
        self.assertEqual(1, len(planned))
        self.assertEqual(evening - 60, planned[0].start)

    def test_one_gate_combines_weekly_schedule_and_shows(self):
        active = self.active(show_entry())
        store_windows(windows_path(active), {CHANNEL: [Window(1000, 2000)]})
        closed, always = normalize_schedule({}), normalize_schedule("continuous")
        inside, outside = (datetime.fromtimestamp(t, timezone.utc) for t in (1500, 3000))
        self.assertTrue(recording_window_open(active, CHANNEL, closed, "UTC", inside))
        self.assertFalse(recording_window_open(active, CHANNEL, closed, "UTC", outside))
        self.assertTrue(recording_window_open(active, CHANNEL, always, "UTC", outside))
        self.assertFalse(recording_window_open({"archive_root": str(self.root)}, CHANNEL, closed, "UTC", inside))
        self.assertTrue(tasks._window_open(active, CHANNEL, closed, "UTC", inside))


if __name__ == "__main__":
    unittest.main()


class PreviewTests(unittest.TestCase):
    def active(self, *entries):
        return {"archive_root": "/unused", "show_rules": list(entries),
                "recording_schedule": {"timezone": "UTC", "channels": {}}}

    def test_preview_lists_upcoming_matches_with_rule_names(self):
        from catchuparr.show_windows import preview_show_matches

        active = self.active(show_entry())
        programmes = {CHANNEL: [
            Programme(0, 600, "Show old"), Programme(1000, 1600, "Other"),
            Programme(2000, 2600, "Show two"), Programme(3000, 3600, "Show three"),
        ]}
        result = preview_show_matches(active, programmes, now=700, limit=1)
        self.assertEqual(
            [{"title": "Show two", "sub_title": "", "start": 2000, "end": 2600, "rules": ["Synthetic"]}],
            result["channels"][CHANNEL],
        )
        self.assertFalse(result["epg_unavailable"])

    def test_preview_flags_an_unreadable_epg(self):
        from catchuparr.show_windows import preview_show_matches

        result = preview_show_matches(self.active(show_entry()), {CHANNEL: None}, now=0)
        self.assertEqual({"channels": {}, "epg_unavailable": True, "timezone": "UTC"}, result)

    def test_validation_attaches_matches_only_for_shows_rules(self):
        from catchuparr import configuration

        active = {"channel_uuids": [], "source_policies": {}, "warnings": [], "show_rules": [show_entry()]}
        with patch.object(configuration, "load_active_configuration", return_value=None), \
                patch.object(configuration, "compile_draft", return_value=(active, None, [])):
            result = configuration.validate_configuration({}, show_preview=lambda a: {"seen": True})
            self.assertEqual({"seen": True}, result["show_matches"])
            active["show_rules"] = []
            self.assertNotIn("show_matches", configuration.validate_configuration(
                {}, show_preview=lambda a: {"seen": True}))


SHOWS = (Show(1000, 1900, "Show A", 1), Show(1900, 2800, "Show B", 1))


class ShowListTests(unittest.TestCase):
    def active(self, *entries):
        return {"archive_root": "/unused", "show_rules": list(entries),
                "recording_schedule": {"timezone": "UTC", "channels": {}}}

    def stored(self, root, *windows):
        from catchuparr.show_windows import store_windows, windows_path

        active = self.active(show_entry())
        active["archive_root"] = root
        store_windows(windows_path(active), {CHANNEL: list(windows)})
        return active

    def test_windows_keep_their_shows_in_the_file_and_old_files_still_load(self):
        with tempfile.TemporaryDirectory() as root:
            active = self.stored(root, Window(940, 2920, (1,), SHOWS))
            self.assertEqual(SHOWS, load_windows(windows_path(active))[CHANNEL][0].shows)
            windows_path(active).write_text(
                '{"channels":{"%s":[[940.0,2920.0,[1]]]}}' % CHANNEL, encoding="utf-8",
            )
            from catchuparr.show_windows import _cache

            _cache.clear()
            self.assertEqual((), load_windows(windows_path(active))[CHANNEL][0].shows)

    def test_running_window_keeps_its_shows_when_the_epg_row_vanishes(self):
        with tempfile.TemporaryDirectory() as root:
            active = self.stored(root, Window(940, 2920, (1,), SHOWS))
            planned = plan_all(active, {CHANNEL: []}, 1500)
        self.assertEqual(SHOWS, planned[CHANNEL][0].shows)

    def test_current_show_names_the_airing_upcoming_and_ended_programme(self):
        from catchuparr.show_windows import current_show

        with tempfile.TemporaryDirectory() as root:
            active = self.stored(root, Window(940, 2920, (1,), SHOWS))
            self.assertEqual("Show A", current_show(active, CHANNEL, 1000)["title"])
            self.assertEqual("airing", current_show(active, CHANNEL, 1000)["phase"])
            self.assertEqual("Synthetic", current_show(active, CHANNEL, 2000)["rule"])
            self.assertEqual("Show B", current_show(active, CHANNEL, 2000)["title"])
            self.assertEqual(("Show A", "upcoming"), tuple(
                current_show(active, CHANNEL, 950)[key] for key in ("title", "phase")))
            self.assertEqual(("Show B", "ended"), tuple(
                current_show(active, CHANNEL, 2850)[key] for key in ("title", "phase")))
            self.assertIsNone(current_show(active, CHANNEL, 3000))
            self.assertIsNone(current_show(active, OTHER, 1000))

    def test_rows_group_channels_and_label_other_days(self):
        from catchuparr.show_windows import format_show_rows

        day = datetime(2026, 11, 2, 12, tzinfo=timezone.utc).timestamp()
        rows = format_show_rows([
            ("B", day + 3600, day + 4500, "News"), ("A", day + 3600, day + 4500, "News"),
            ("A", day + 90000, day + 90900, "News"),
        ], "UTC", day)
        self.assertEqual(["13:00–13:15 News › A, B", "Tue 13:00–13:15 News › A"], rows)

    def test_validation_pushes_the_matches_as_a_toast(self):
        from catchuparr import runtime

        pushed = {}

        def fake(rows, **kwargs):
            pushed["rows"], pushed["title"] = list(rows), kwargs["title"]
            return "summary"

        result = {"message": "1 channel(s) selected.", "channels": [{"channel_uuid": CHANNEL, "channel_name": "Ch"}]}
        matches = {"channels": {CHANNEL: [{"title": "Show A", "start": 0, "end": 900, "rules": ["x"]}]},
                   "epg_unavailable": False, "timezone": "UTC"}
        with patch("catchuparr.action_messages.deliver_action_message", fake):
            self.assertEqual("summary", runtime.deliver_show_matches(result, matches))
        self.assertEqual("Shows", pushed["title"])
        self.assertEqual(1, len(pushed["rows"]))
        self.assertIn("Show A › Ch", pushed["rows"][0])

    def test_validation_toast_lists_only_the_next_matches_per_channel(self):
        from catchuparr import runtime

        pushed = {}

        def fake(rows, **kwargs):
            pushed["rows"] = list(rows)
            return kwargs["summary"]

        items = [{"title": f"Show {n}", "start": n * 1000, "end": n * 1000 + 900, "rules": ["x"]} for n in range(8)]
        result = {"message": "1 channel(s) selected.", "channels": [{"channel_uuid": CHANNEL, "channel_name": "Ch"}]}
        matches = {"channels": {CHANNEL: items}, "epg_unavailable": True, "timezone": "UTC"}
        with patch("catchuparr.action_messages.deliver_action_message", fake):
            summary = runtime.deliver_show_matches(result, matches)
        self.assertEqual(5, len(pushed["rows"]))
        self.assertIn("8 upcoming shows match (the next 5 per channel are listed)", summary)
        self.assertTrue(summary.endswith("could not be read for some channels."))
