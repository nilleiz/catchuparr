import unittest

from catchuparr.source_rules import SourceRuleError, compile_source_rules, rank_candidates


class SourceRulesTests(unittest.TestCase):
    def setUp(self):
        self.channels = [
            {"uuid": "uuid-1", "number": "1", "name": "Das Erste HD", "group": "News"},
            {"uuid": "uuid-2", "number": "3.5", "name": "ZDF", "group": "News"},
            {"uuid": "uuid-3", "number": "10", "name": "Kultur", "group": "Arts"},
            {"uuid": "uuid-4", "number": "20", "name": "Sport", "group": "Sports"},
        ]
        self.accounts = [
            {"id": "a", "name": "Vu+"},
            {"id": "b", "name": "Waipu"},
            {"id": "c", "name": "Zattoo"},
        ]

    def test_selector_union_and_global_fallback_replacement(self):
        policies = compile_source_rules(
            '* | mode=exclude-only | m3u="Zattoo"\n'
            'number:1,10-20 | mode=include-only | m3u="Vu+"\n'
            'name:"ZDF" | mode=unchanged',
            self.channels,
            self.accounts,
        )

        self.assertEqual(set(policies), {channel["uuid"] for channel in self.channels})
        self.assertEqual(policies["uuid-1"].mode, "include-only")
        self.assertEqual(policies["uuid-3"].mode, "include-only")
        self.assertEqual(policies["uuid-4"].mode, "include-only")
        self.assertEqual(policies["uuid-2"].mode, "unchanged")
        self.assertEqual(policies["uuid-1"].account_ids, frozenset({"a"}))

    def test_group_selects_all_exact_group_members(self):
        policies = compile_source_rules(
            'group:"News" | mode=unchanged', self.channels, self.accounts
        )

        self.assertEqual(set(policies), {"uuid-1", "uuid-2"})

    def test_number_ranges_use_decimal_bounds_and_union_deduplicates(self):
        policies = compile_source_rules(
            'number:1,1.0-3.5,10-20 | mode=unchanged', self.channels, self.accounts
        )

        self.assertEqual(set(policies), {"uuid-1", "uuid-2", "uuid-3", "uuid-4"})

    def test_number_rules_skip_channels_with_blank_catalog_numbers(self):
        channels = [
            {"uuid": "uuid-decimal", "number": "1.25", "name": "Decimal", "group": "News"},
            {"uuid": "uuid-none", "number": None, "name": "No number", "group": "News"},
            {"uuid": "uuid-blank", "number": "  ", "name": "Blank number", "group": "News"},
        ]

        policies = compile_source_rules(
            "number:1.25,1.20-1.30 | mode=unchanged", channels, self.accounts
        )

        self.assertEqual(set(policies), {"uuid-decimal"})

    def test_priority_ranks_by_score_and_preserves_original_order_for_ties(self):
        policy = compile_source_rules(
            '* | mode=priority | priority="Vu+":100,"Waipu":50',
            self.channels,
            self.accounts,
        )["uuid-1"]
        streams = [
            {"id": "waipu-first", "account_id": "b", "order": 0},
            {"id": "zattoo", "account_id": "c", "order": 1},
            {"id": "vu-second", "account_id": "a", "order": 2},
            {"id": "waipu-second", "account_id": "b", "order": 3},
            {"id": "unassigned", "account_id": None, "order": 4},
            {"id": "unknown-account", "account_id": "missing", "order": 5},
        ]

        self.assertEqual(
            [stream["id"] for stream in rank_candidates(policy, streams)],
            ["vu-second", "waipu-first", "waipu-second", "zattoo"],
        )
        with self.assertRaises((AttributeError, TypeError)):
            policy.mode = "unchanged"

    def test_include_and_exclude_filter_only_resolved_assigned_streams(self):
        streams = [
            {"id": "a", "account_id": "a", "order": 0},
            {"id": "b", "account_id": "b", "order": 1},
            {"id": "unassigned", "account_id": None, "order": 2},
            {"id": "unknown", "account_id": "not-an-account", "order": 3},
        ]
        include = compile_source_rules(
            '* | mode=include-only | m3u="Vu+"', self.channels, self.accounts
        )["uuid-1"]
        exclude = compile_source_rules(
            '* | mode=exclude-only | m3u="Vu+"', self.channels, self.accounts
        )["uuid-1"]

        self.assertEqual([item["id"] for item in rank_candidates(include, streams)], ["a"])
        self.assertEqual([item["id"] for item in rank_candidates(exclude, streams)], ["b"])

    def test_rejects_unknown_ambiguous_and_overlapping_selectors(self):
        invalid = [
            ('number:2 | mode=unchanged', "matches no channels"),
            ('name:"Missing" | mode=unchanged', "matches no channels"),
            ('group:"Missing" | mode=unchanged', "matches no channels"),
            ('number:30-40 | mode=unchanged', "matches no channels"),
            ('number:1 | mode=unchanged\nname:"Das Erste HD" | mode=unchanged', "overlap"),
            ('* | mode=unchanged\n* | mode=unchanged', "only one global"),
        ]
        for rule, message in invalid:
            with self.subTest(rule=rule), self.assertRaisesRegex(SourceRuleError, message):
                compile_source_rules(rule, self.channels, self.accounts)

    def test_rejects_ambiguous_names_numbers_and_account_names(self):
        duplicate_channels = [
            *self.channels,
            {"uuid": "uuid-5", "number": "1.0", "name": "Das Erste HD", "group": "Other"},
        ]
        duplicate_accounts = [*self.accounts, {"id": "d", "name": "Vu+"}]
        invalid = [
            ('number:1 | mode=unchanged', duplicate_channels, self.accounts, "number 1 is ambiguous"),
            ('name:"Das Erste HD" | mode=unchanged', duplicate_channels, self.accounts,
             "name 'Das Erste HD' is ambiguous"),
            ('number:1-3 | mode=unchanged', duplicate_channels, self.accounts,
             "number range 1..3 is ambiguous"),
            ('* | mode=include-only | m3u="Vu+"', self.channels, duplicate_accounts,
             "ambiguous M3U account name"),
            ('* | mode=include-only | m3u="Missing"', self.channels, self.accounts,
             "unknown M3U account name"),
        ]
        for rule, channels, accounts, message in invalid:
            with self.subTest(rule=rule), self.assertRaisesRegex(SourceRuleError, message):
                compile_source_rules(rule, channels, accounts)


if __name__ == "__main__":
    unittest.main()
