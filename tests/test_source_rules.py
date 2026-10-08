import unittest

from catchuparr.source_rules import (
    MAX_FILTER_CONFIG_BYTES,
    SourceRuleError,
    compile_filter_config,
    rank_candidates,
)

CHANNEL_A = "00000000-0000-0000-0000-000000000001"
CHANNEL_B = "00000000-0000-0000-0000-000000000002"
CHANNEL_C = "00000000-0000-0000-0000-000000000003"


class SourceRulesTests(unittest.TestCase):
    def setUp(self):
        self.channels = [
            {"uuid": CHANNEL_A, "number": "1", "name": "Synthetic Channel A", "group": "News"},
            {"uuid": CHANNEL_B, "number": "3.5", "name": "Synthetic Channel B", "group": "News"},
            {"uuid": CHANNEL_C, "number": "10", "name": "Synthetic Channel C", "group": "Arts"},
        ]
        self.accounts = [
            {"id": "11", "name": "Synthetic Source A"},
            {"id": "12", "name": "Synthetic Source B"},
            {"id": "13", "name": "Synthetic Source C"},
        ]
        self.profiles = [
            {"id": "4", "name": "Synthetic Profile All", "channel_uuids": (CHANNEL_A, CHANNEL_B)},
            {"id": "5", "name": "Synthetic Profile Limited", "channel_uuids": (CHANNEL_B,)},
            {"id": "6", "name": "Disabled Memberships", "channel_uuids": ()},
        ]
        self.streams = {
            CHANNEL_A: (
                {"id": "a1", "account_id": "11", "order": 0},
                {"id": "a2", "account_id": "12", "order": 1},
                {"id": "a3", "account_id": "13", "order": 2},
            ),
            CHANNEL_B: (
                {"id": "b1", "account_id": "12", "order": 0},
                {"id": "b2", "account_id": "11", "order": 1},
            ),
            CHANNEL_C: ({"id": "c1", "account_id": "11", "order": 0},),
        }

    def compile(self, text, **kwargs):
        return compile_filter_config(
            text,
            kwargs.get("channels", self.channels),
            kwargs.get("accounts", self.accounts),
            kwargs.get("profiles", self.profiles),
            kwargs.get("streams", self.streams),
        )

    def test_blank_and_empty_rules_select_no_channels(self):
        self.assertEqual((), self.compile("").channel_uuids)
        result = self.compile("version: 1\nprofile: all\nrules: []\n")
        self.assertEqual((), result.channel_uuids)

    def test_outer_profile_defaults_to_all(self):
        omitted = self.compile(
            "version: 1\nrules:\n  - channels: {numbers: [1]}\n"
        )
        explicit = self.compile(
            "version: 1\nprofile: all\nrules:\n  - channels: {numbers: [1]}\n"
        )
        self.assertEqual(explicit, omitted)

    def test_channel_profile_scope_uses_only_enabled_members(self):
        result = self.compile(
            "version: 1\nprofile: Synthetic Profile All\nrules:\n  - channels:\n      profile: all\n"
        )
        self.assertEqual((CHANNEL_A, CHANNEL_B), result.channel_uuids)
        self.assertEqual(("4",), result.profile_ids)

    def test_specific_rule_fully_overrides_baseline_filter(self):
        result = self.compile(
            "version: 1\n"
            "profile: all\n"
            "rules:\n"
            "  - channels: {profile: all}\n"
            "    exclude: [Synthetic Source C]\n"
            "  - channels: {names: [Synthetic Channel B]}\n"
            "    include: [Synthetic Source B]\n"
        )
        self.assertEqual((CHANNEL_A, CHANNEL_B), result.channel_uuids)
        self.assertEqual(frozenset({"12"}), result.source_policies[CHANNEL_B].include_account_ids)
        self.assertEqual(frozenset({"13"}), result.source_policies[CHANNEL_A].exclude_account_ids)
        self.assertEqual(["b1"], [row["stream_id"] for row in result.channels[1]["candidates"]])

    def test_selector_numbers_names_groups_and_profiles(self):
        number = self.compile(
            "version: 1\nprofile: all\nrules:\n  - channels:\n      numbers: [1, '3.5']\n"
        )
        self.assertEqual((CHANNEL_A, CHANNEL_B), number.channel_uuids)
        group = self.compile(
            "version: 1\nprofile: all\nrules:\n  - channels: {groups: [News]}\n"
        )
        self.assertEqual((CHANNEL_A, CHANNEL_B), group.channel_uuids)
        profile = self.compile(
            "version: 1\nprofile: all\nrules:\n  - channels: {profile: Synthetic Profile Limited}\n"
        )
        self.assertEqual((CHANNEL_B,), profile.channel_uuids)
        self.assertEqual(("4", "5"), profile.profile_ids)

    def test_priority_sorts_stably_and_filters_only_assigned_rows(self):
        result = self.compile(
            "version: 1\n"
            "profile: all\n"
            "rules:\n"
            "  - channels: {numbers: [1]}\n"
            "    exclude: []\n"
            "    priority: {Synthetic Source B: 20, Synthetic Source C: 20}\n"
        )
        policy = result.source_policies[CHANNEL_A]
        streams = [
            {"id": "b", "account_id": "12", "order": 0},
            {"id": "c", "account_id": "13", "order": 1},
            {"id": "a", "account_id": "11", "order": 2},
            {"id": "unknown", "account_id": "999", "order": 3},
        ]
        self.assertEqual(["b", "c", "a"], [row["id"] for row in rank_candidates(policy, streams)])

    def test_empty_exclude_without_priority_keeps_shared_route(self):
        result = self.compile(
            "version: 1\nprofile: all\nrules:\n  - channels: {profile: all}\n    exclude: []\n"
        )
        self.assertEqual({}, result.source_policies)
        self.assertTrue(all(not row["source_override"] for row in result.channels))

    def test_explicit_include_stays_private_when_current_assignment_already_matches(self):
        result = self.compile(
            "version: 1\nprofile: all\nrules:\n"
            "  - channels: {numbers: [1]}\n"
            "    include: [Synthetic Source A, Synthetic Source B, Synthetic Source C]\n"
        )
        self.assertEqual(
            ["a1", "a2", "a3"],
            [row["stream_id"] for row in result.channels[0]["candidates"]],
        )
        self.assertEqual(
            frozenset({"11", "12", "13"}),
            result.source_policies[CHANNEL_A].include_account_ids,
        )
        self.assertTrue(result.channels[0]["source_override"])

    def test_priority_stays_private_when_current_assignment_already_matches(self):
        result = self.compile(
            "version: 1\nprofile: all\nrules:\n"
            "  - channels: {numbers: [1]}\n"
            "    exclude: []\n"
            "    priority: {Synthetic Source A: 100}\n"
        )
        self.assertEqual(
            ["a1", "a2", "a3"],
            [row["stream_id"] for row in result.channels[0]["candidates"]],
        )
        self.assertTrue(result.channels[0]["source_override"])
        self.assertIn(CHANNEL_A, result.source_policies)

    def test_rejects_overlap_unknown_or_ambiguous_selectors(self):
        invalid = (
            (
                "version: 1\nprofile: all\nrules:\n  - channels: {numbers: [1]}\n"
                "  - channels: {names: [Synthetic Channel A]}\n",
                "overlap",
            ),
            ("version: 1\nprofile: all\nrules:\n  - channels: {numbers: [99]}\n", "matches no channels"),
            ("version: 1\nprofile: Missing\nrules: []\n", "unknown channel profile"),
            (
                "version: 1\nprofile: all\nrules:\n  - channels: {profile: Disabled Memberships}\n",
                "no eligible channels",
            ),
            (
                "version: 1\nprofile: Disabled Memberships\nrules:\n"
                "  - channels: {profile: all}\n",
                "no eligible channels",
            ),
        )
        for document, message in invalid:
            with self.subTest(message=message), self.assertRaisesRegex(SourceRuleError, message):
                self.compile(document)

    def test_rejects_filter_errors_with_rule_field_and_line(self):
        invalid = (
            (
                "version: 1\nprofile: all\nrules:\n  - channels: {profile: all}\n    include: []\n",
                r"line 4, field include",
            ),
            (
                "version: 1\nprofile: all\nrules:\n  - channels: {profile: all}\n    include: [Missing]\n",
                r"rule 1: line 4, field include.*unknown M3U account",
            ),
            (
                "version: 1\nprofile: all\nrules:\n  - channels: {profile: all}\n    priority: {Synthetic Source A: 1}\n",
                r"field priority.*requires include or exclude",
            ),
        )
        for document, message in invalid:
            with self.subTest(message=message), self.assertRaisesRegex(SourceRuleError, message):
                self.compile(document)

    def test_rejects_priorities_forbidden_by_include_or_exclude(self):
        invalid = (
            (
                "version: 1\nprofile: all\nrules:\n"
                "  - channels: {numbers: [1]}\n"
                "    include: [Synthetic Source A]\n"
                "    priority: {Synthetic Source B: 10}\n",
                "excluded by include filter",
            ),
            (
                "version: 1\nprofile: all\nrules:\n"
                "  - channels: {numbers: [1]}\n"
                "    exclude: [Synthetic Source B]\n"
                "    priority: {Synthetic Source B: 10}\n",
                "excluded by source filter",
            ),
        )
        for document, message in invalid:
            with self.subTest(message=message), self.assertRaisesRegex(SourceRuleError, message):
                self.compile(document)

    def test_rejects_duplicate_unknown_alias_anchor_merge_and_unsafe_tags(self):
        invalid_documents = (
            "version: 1\nversion: 1\nprofile: all\nrules: []\n",
            "version: 1\nprofile: all\nrules: []\nunknown: true\n",
            "version: 1\nprofile: all\nrules: &rules []\n",
            "version: 1\nprofile: all\nrules: *rules\n",
            "version: 1\nprofile: all\nrules: []\nextra: {<<: {x: 1}}\n",
            "version: 1\nprofile: all\nrules: !!python/object/apply:os.system ['true']\n",
        )
        for document in invalid_documents:
            with self.subTest(document=document), self.assertRaises(SourceRuleError):
                self.compile(document)
        with self.assertRaisesRegex(SourceRuleError, "64 KiB"):
            self.compile(" " * (MAX_FILTER_CONFIG_BYTES + 1))

    def test_rejects_ambiguous_catalog_names(self):
        duplicate_channels = [
            *self.channels,
            {"uuid": "00000000-0000-0000-0000-000000000004", "number": "1", "name": "Synthetic Channel A", "group": "Other"},
        ]
        duplicate_profiles = [
            *self.profiles,
            {
                "id": "9",
                "name": "Synthetic Profile Duplicate",
                "channel_uuids": ("00000000-0000-0000-0000-000000000004",),
            },
        ]
        duplicate_accounts = [*self.accounts, {"id": "14", "name": "Synthetic Source A"}]
        with self.assertRaisesRegex(SourceRuleError, "ambiguous"):
            self.compile(
                "version: 1\nprofile: all\nrules:\n  - channels: {numbers: [1]}\n",
                channels=duplicate_channels,
                profiles=duplicate_profiles,
            )
        with self.assertRaisesRegex(SourceRuleError, "ambiguous"):
            self.compile(
                "version: 1\nprofile: all\nrules:\n  - channels: {profile: all}\n    include: [Synthetic Source A]\n",
                accounts=duplicate_accounts,
            )


if __name__ == "__main__":
    unittest.main()
