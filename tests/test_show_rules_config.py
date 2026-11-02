import unittest

from catchuparr.source_rules import SourceRuleError, compile_filter_config

A = "00000000-0000-0000-0000-000000000001"
B = "00000000-0000-0000-0000-000000000002"
C = "00000000-0000-0000-0000-000000000003"


class ShowRuleConfigTests(unittest.TestCase):
    def setUp(self):
        self.channels = [
            {"uuid": A, "number": "1", "name": "Synthetic Channel A", "group": "News"},
            {"uuid": B, "number": "2", "name": "Synthetic Channel B", "group": "News"},
            {"uuid": C, "number": "3", "name": "Synthetic Channel C", "group": "Arts"},
        ]
        self.accounts = [{"id": "11", "name": "Source A"}, {"id": "12", "name": "Source B"}]
        self.profiles = [{"id": "4", "name": "All", "channel_uuids": (A, B, C)}]
        self.streams = {
            A: ({"id": "a1", "account_id": "11", "order": 0}, {"id": "a2", "account_id": "12", "order": 1}),
            B: ({"id": "b1", "account_id": "12", "order": 0},),
            C: ({"id": "c1", "account_id": "11", "order": 0},),
        }

    def compile(self, text):
        return compile_filter_config(text, self.channels, self.accounts, self.profiles, self.streams)

    def test_show_rule_selects_channels_and_gets_an_empty_weekly_schedule(self):
        result = self.compile(
            "version: 1\nrules:\n  - shows:\n      name: Example\n"
            "      match: {title: {starts_with: News}}\n"
            "      channels:\n        include: {groups: [News]}\n        exclude: {names: [Synthetic Channel B]}\n"
            "      padding: {before: 2, after: 10}\n"
        )
        self.assertEqual((A,), result.channel_uuids)
        self.assertEqual({"mode": "weekly", "intervals": []}, result.channel_schedules[A].to_snapshot())
        (show,) = result.show_rules
        self.assertEqual("Example", show["name"])
        self.assertEqual([A], show["channel_uuids"])
        self.assertEqual({"before": 2.0, "after": 10.0}, show["padding"])
        self.assertEqual({"title": {"starts_with": "News"}}, show["match"])
        self.assertIsNone(show["schedule"])

    def test_default_scope_is_every_eligible_channel_with_epg(self):
        result = self.compile("version: 1\nrules:\n  - shows:\n      match: {title: {contains: x}}\n")
        self.assertEqual((A, B, C), result.channel_uuids)
        self.channels[1]["has_epg"] = False
        result = self.compile("version: 1\nrules:\n  - shows:\n      match: {title: {contains: x}}\n")
        self.assertEqual((A, C), result.channel_uuids)

    def test_ignored_m3u_filter_is_reported(self):
        result = self.compile(
            "version: 1\nrules:\n  - channels: {names: [Synthetic Channel A]}\n"
            "  - shows:\n      match: {title: {contains: x}}\n    m3u: {include: [Source B]}\n"
        )
        self.assertEqual(1, len(result.warnings))
        self.assertIn("does not apply to 1 channel", result.warnings[0])

    def test_channel_rule_keeps_its_schedule_and_policy_next_to_shows(self):
        result = self.compile(
            "version: 1\nrules:\n"
            "  - channels: {names: [Synthetic Channel A]}\n    include: [Source A]\n"
            "    schedule: {daily: [{start: '10:00', end: '11:00'}]}\n"
            "  - shows:\n      match: {title: {contains: x}}\n      channels: {include: {names: [Synthetic Channel A, Synthetic Channel C]}}\n"
            "    m3u: {include: [Source B]}\n"
        )
        self.assertEqual((A, C), result.channel_uuids)
        self.assertEqual("weekly", result.channel_schedules[A].mode)
        self.assertNotEqual([], result.channel_schedules[A].to_snapshot()["intervals"])
        self.assertEqual({A, C}, set(result.source_policies))
        self.assertEqual(frozenset({"11"}), result.source_policies[A].include_account_ids)
        self.assertEqual(frozenset({"12"}), result.source_policies[C].include_account_ids)

    def test_shows_schedule_is_a_start_restriction(self):
        result = self.compile(
            "version: 1\nrules:\n  - shows:\n      match: {title: {contains: x}}\n"
            "    schedule: {weekdays: [{start: '18:00', end: '23:00'}]}\n"
        )
        self.assertEqual("weekly", result.show_rules[0]["schedule"]["mode"])

    def test_nested_m3u_equals_flat_filter_and_flat_warns(self):
        nested = self.compile(
            "version: 1\nrules:\n  - channels: {names: [Synthetic Channel A]}\n"
            "    m3u: {include: [Source B], priority: {Source B: 5}}\n"
        )
        flat = self.compile(
            "version: 1\nrules:\n  - channels: {names: [Synthetic Channel A]}\n"
            "    include: [Source B]\n    priority: {Source B: 5}\n"
        )
        self.assertEqual(nested.source_policies, flat.source_policies)
        self.assertEqual((), nested.warnings)
        self.assertEqual(1, len(flat.warnings))

    def test_invalid_rules(self):
        cases = {
            "both": "rules:\n  - channels: {profile: all}\n    shows: {match: {title: {contains: x}}}\n",
            "neither": "rules:\n  - include: [Source A]\n",
            "mixed": "rules:\n  - channels: {profile: all}\n    m3u: {include: [Source A]}\n    exclude: [Source B]\n",
            "bad regex": "rules:\n  - shows: {match: {title: {regex: '('}}}\n",
            "no match": "rules:\n  - shows: {name: x}\n",
            "padding": "rules:\n  - shows: {match: {title: {contains: x}}, padding: {before: 999}}\n",
            "unknown key": "rules:\n  - shows: {match: {title: {contains: x}}, when: now}\n",
            "mode": "rules:\n  - shows: {match: {title: {contains: x}}}\n    schedule: {daily: []}\n    schedule_mode: replace\n",
            "all excluded": "rules:\n  - shows: {match: {title: {contains: x}}, channels: {exclude: {profile: all}}}\n",
            "competing": (
                "rules:\n  - shows: {match: {title: {contains: x}}}\n    m3u: {include: [Source A]}\n"
                "  - shows: {match: {title: {contains: y}}}\n    m3u: {include: [Source B]}\n"
            ),
            "priority": "rules:\n  - shows: {match: {title: {contains: x}}}\n    m3u: {priority: {Source A: 1}}\n",
            "priority outside include": (
                "rules:\n  - shows: {match: {title: {contains: x}}}\n"
                "    m3u: {include: [Source A], priority: {Source B: 1}}\n"
            ),
            "priority excluded": (
                "rules:\n  - shows: {match: {title: {contains: x}}}\n"
                "    m3u: {exclude: [Source B], priority: {Source B: 1}}\n"
            ),
        }
        for name, body in cases.items():
            with self.subTest(name), self.assertRaises(SourceRuleError):
                self.compile("version: 1\n" + body)


if __name__ == "__main__":
    unittest.main()
