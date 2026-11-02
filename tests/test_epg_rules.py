import unittest

from catchuparr.epg_rules import (
    Programme,
    Show,
    ShowRule,
    ShowRuleError,
    Window,
    compile_match,
    compile_padding,
    freeze_running,
    merge_windows,
    next_window_start,
    plan_windows,
    window_active,
)


def programme(title, start=0, end=1800, **extra):
    return Programme(start, end, title, **extra)


def matches(spec, **fields):
    return compile_match(spec)(programme(**fields))


class MatchTests(unittest.TestCase):
    def test_text_operators(self):
        self.assertTrue(matches({"title": {"starts_with": "News"}}, title="NEWS at six"))
        self.assertFalse(matches({"title": {"starts_with": "six"}}, title="News at six"))
        self.assertTrue(matches({"title": {"ends_with": "six"}}, title="News at six"))
        self.assertTrue(matches({"title": {"contains": "at"}}, title="News at six"))
        self.assertTrue(matches({"title": {"includes": "at"}}, title="News at six"))
        self.assertTrue(matches({"title": {"equals": "news   at six"}}, title="News at six"))
        self.assertFalse(matches({"title": {"equals": "News"}}, title="News at six"))

    def test_wildcard_and_regex(self):
        self.assertTrue(matches({"title": {"wildcard": "News*six"}}, title="News at six"))
        self.assertFalse(matches({"title": {"wildcard": "News?six"}}, title="News at six"))
        self.assertTrue(matches({"title": {"regex": r"^News( at \d+| at six)$"}}, title="News at six"))
        self.assertFalse(matches({"title": {"regex": r"^six"}}, title="News at six"))

    def test_lists_and_combinators(self):
        spec = {"title": {"any": [{"starts_with": "A"}, {"regex": "^B+$"}], "not": {"contains": "x"}}}
        self.assertTrue(matches(spec, title="Alpha"))
        self.assertTrue(matches(spec, title="BBB"))
        self.assertFalse(matches(spec, title="Axe"))
        self.assertFalse(matches(spec, title="Gamma"))
        self.assertTrue(matches({"title": {"contains": ["x", "y"]}}, title="gym"))
        self.assertTrue(matches({"title": {"all": [{"contains": "a"}, {"contains": "b"}]}}, title="cab"))

    def test_case_sensitivity_and_unicode(self):
        spec = {"title": {"contains": "Straße"}}
        self.assertTrue(matches(spec, title="STRASSE live"))
        self.assertFalse(matches({**spec, "case_sensitive": True}, title="strasse"))
        self.assertTrue(matches({"title": {"equals": "ＡＢ"}}, title="ab"))

    def test_regex_ignores_case_without_casefolding_letters(self):
        self.assertTrue(matches({"title": {"regex": "Straße"}}, title="straße live"))
        self.assertTrue(matches({"title": {"regex": "^ab c$"}}, title="AB   c"))
        self.assertFalse(matches({"title": {"regex": "^ab"}, "case_sensitive": True}, title="AB"))

    def test_long_descriptions_are_searched_beyond_the_first_characters(self):
        text = "x " * 400 + "report"
        self.assertTrue(matches({"description": {"contains": "report"}}, title="", description=text))

    def test_fields_combine_and_length_limits(self):
        spec = {"title": {"contains": "doc"}, "description": {"contains": "nature"}, "min_minutes": 20}
        self.assertTrue(matches(spec, title="Doc", description="About nature"))
        self.assertFalse(matches(spec, title="Doc", description="About cities"))
        self.assertFalse(compile_match(spec)(Programme(0, 600, "Doc", description="nature")))
        self.assertFalse(compile_match({"title": {"contains": "d"}, "max_minutes": 10})(programme("d")))

    def test_invalid_specs(self):
        for spec in (
            [], {}, {"channel": {}}, {"title": "News"}, {"title": {}}, {"title": {"starts": "a"}},
            {"title": {"contains": ""}}, {"title": {"regex": "("}}, {"title": {"regex": "(a+)+"}},
            {"title": {"regex": "a" * 201}}, {"title": {"any": []}}, {"title": {"contains": "a"}, "case_sensitive": 1},
            {"title": {"contains": "a"}, "min_minutes": 0}, {"title": {"contains": "a"}, "min_minutes": 9, "max_minutes": 3},
        ):
            with self.subTest(spec=spec), self.assertRaises(ShowRuleError):
                compile_match(spec)

    def test_padding(self):
        self.assertEqual((0.0, 0.0), compile_padding(None))
        self.assertEqual((120.0, 600.0), compile_padding({"before": 2, "after": 10}))
        for spec in ({"before": -1}, {"after": 241}, {"before": 1.5}, {"x": 1}, []):
            with self.subTest(spec=spec), self.assertRaises(ShowRuleError):
                compile_padding(spec)


def rule(index, spec, before=0, after=0, allowed=None):
    return ShowRule(index, compile_match(spec), before * 60.0, after * 60.0, allowed)


class WindowTests(unittest.TestCase):
    def test_padding_and_consecutive_programmes_merge(self):
        rules = [rule(0, {"title": {"starts_with": "Show"}}, before=1, after=2)]
        windows = plan_windows([
            programme("Show 1", 1000, 2800), programme("Show 2", 2800, 4600), programme("Other", 4600, 5200),
        ], rules)
        self.assertEqual([Window(940, 4720, (0,), (
            Show(1000, 2800, "Show 1", 0), Show(2800, 4600, "Show 2", 0),
        ))], windows)
        apart = plan_windows([programme("Show 1", 1000, 2800), programme("Show 2", 3400, 5200)], rules)
        self.assertEqual([
            Window(940, 2920, (0,), (Show(1000, 2800, "Show 1", 0),)),
            Window(3340, 5320, (0,), (Show(3400, 5200, "Show 2", 0),)),
        ], apart)

    def test_touching_programmes_merge_across_rounding_gap(self):
        rules = [rule(0, {"title": {"starts_with": "Show"}})]
        self.assertEqual(1, len(plan_windows([programme("Show 1", 0, 1800), programme("Show 2", 1850, 3600)], rules)))
        self.assertEqual(2, len(plan_windows([programme("Show 1", 0, 1800), programme("Show 2", 1900, 3600)], rules)))

    def test_rules_union_and_start_restriction(self):
        rules = [
            rule(0, {"title": {"contains": "a"}}),
            rule(1, {"title": {"contains": "a"}}, allowed=lambda start: start >= 5000),
        ]
        windows = plan_windows([programme("a", 0, 1000), programme("a", 5000, 6000)], rules)
        self.assertEqual([
            Window(0, 1000, (0,), (Show(0, 1000, "a", 0),)),
            Window(5000, 6000, (0, 1), (Show(5000, 6000, "a", 0), Show(5000, 6000, "a", 1))),
        ], windows)

    def test_invalid_programme_bounds_are_ignored(self):
        self.assertEqual([], plan_windows([programme("a", 10, 10)], [rule(0, {"title": {"contains": "a"}})]))

    def test_running_window_survives_epg_refresh(self):
        previous = [Window(100, 1000, (0,))]
        self.assertEqual([Window(100, 1000, (0,))], freeze_running(previous, [], 500))
        self.assertEqual([Window(100, 1200, (0,))], freeze_running(previous, [Window(300, 1200, (0,))], 500))
        self.assertEqual([Window(100, 1000, (0,))], freeze_running(previous, [Window(200, 800, (0,))], 500))
        self.assertEqual([], freeze_running(previous, [], 1000))
        # Admission opens a window early; it is protected from then on.
        self.assertEqual([Window(100, 1000, (0,))], freeze_running(previous, [], 80))
        self.assertEqual([], freeze_running(previous, [], 50))
        self.assertEqual([Window(2000, 3000, (0,))], freeze_running([], [Window(2000, 3000, (0,))], 500))

    def test_activity_lookahead_and_next_start(self):
        windows = [Window(1000, 2000), Window(5000, 6000)]
        self.assertFalse(window_active(windows, 900))
        self.assertTrue(window_active(windows, 970))
        self.assertTrue(window_active(windows, 1999))
        self.assertFalse(window_active(windows, 2000))
        self.assertEqual(5000, next_window_start(windows, 2000))
        self.assertIsNone(next_window_start(windows, 6000))
        self.assertEqual([], merge_windows([]))


if __name__ == "__main__":
    unittest.main()
