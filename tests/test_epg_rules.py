import unittest

from catchuparr.epg_rules import (
    Neighbour,
    Programme,
    Show,
    ShowRule,
    ShowRuleError,
    Window,
    compile_match,
    compile_neighbour,
    compile_padding,
    freeze_running,
    merge_windows,
    neighbour_snapshot,
    next_window_start,
    plan_windows,
    preview_matches,
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


def neighbours(previous=None, nxt=None, before=0, after=0):
    return ShowRule(
        0, compile_match({"title": {"equals": "Show"}}), before * 60.0, after * 60.0,
        previous=compile_neighbour(previous, field="include_previous"),
        next=compile_neighbour(nxt, field="include_next"),
    )


# 00:00 Magazine (30 min), 00:30 Weather (5 min), 00:35 Show (30 min),
# 01:05 Short (5 min), 01:10 Film (90 min); seconds from 00:00.
GUIDE = [
    programme("Earlier", -1800, 0), programme("Magazine", 0, 1800), programme("Weather", 1800, 2100),
    programme("Show", 2100, 3900), programme("Short", 3900, 4200), programme("Film", 4200, 9600),
]


def span(rule, guide=GUIDE):
    (window,) = plan_windows(guide, [rule])
    return window.start, window.end


class NeighbourTests(unittest.TestCase):
    def test_without_options_only_the_match_is_recorded(self):
        self.assertEqual((2100, 3900), span(neighbours()))

    def test_whole_previous_and_next_programme(self):
        self.assertEqual((1800, 4200), span(neighbours(True, True)))

    def test_min_adds_exactly_one_more_programme(self):
        self.assertEqual((0, 3900), span(neighbours({"min": 10})))       # Weather < 10 min -> Magazine too
        self.assertEqual((1800, 3900), span(neighbours({"min": 5})))     # Weather is not shorter than 5
        self.assertEqual((2100, 9600), span(neighbours(None, {"min": 10})))
        # Magazine (30 min) is shorter than 40, but never a third programme.
        self.assertEqual((0, 3900), span(neighbours({"min": 40})))

    def test_max_caps_the_whole_lead_and_tail(self):
        self.assertEqual((900, 3900), span(neighbours({"min": 10, "max": 20})))
        self.assertEqual((2100, 5700), span(neighbours(None, {"min": 10, "max": 30})))
        self.assertEqual((1800, 3900), span(neighbours({"max": 60})))   # max above the neighbour length

    def test_padding_applies_to_the_ends_of_the_neighbours(self):
        self.assertEqual((1800 - 120, 4200 + 300), span(neighbours(True, True, before=2, after=5)))

    def test_gaps_and_missing_neighbours(self):
        gap = [programme("Weather", 1800, 2000), programme("Show", 2100, 3900)]
        self.assertEqual((2100, 3900), span(neighbours(True, True), gap))
        touching = [programme("Weather", 1800, 2070), programme("Show", 2100, 3900)]
        self.assertEqual((1800, 3900), span(neighbours(True), touching))
        self.assertEqual((2100, 3900), span(neighbours(True, True), [programme("Show", 2100, 3900)]))

    def test_neighbour_spans_merge_with_another_match(self):
        guide = [programme("Show", 0, 1800), programme("Between", 1800, 3600), programme("Show", 3600, 5400)]
        (window,) = plan_windows(guide, [neighbours(None, True)])
        self.assertEqual((0, 5400), (window.start, window.end))
        self.assertEqual(["Show", "Show"], [show.title for show in window.shows])

    def test_preview_reports_the_recording_span(self):
        (match,) = [m for m in preview_matches(GUIDE, [neighbours({"min": 10}, True, before=2)], now=2000)]
        self.assertEqual((2100, 3900), (match["start"], match["end"]))
        self.assertEqual((0 - 120, 4200), (match["record_start"], match["record_end"]))
        self.assertTrue(match["neighbours"])

    def test_neighbour_options_are_validated(self):
        self.assertIsNone(compile_neighbour(None, field="f"))
        self.assertIsNone(compile_neighbour(False, field="f"))
        self.assertEqual(Neighbour(), compile_neighbour(True, field="f"))
        self.assertEqual(Neighbour(600.0, 3600.0), compile_neighbour({"min": 10, "max": 60}, field="f"))
        for bad in ({"min": 0}, {"max": 1441}, {"min": 30, "max": 10}, {"later": 1}, {"min": "5"}, "yes", 3):
            with self.subTest(bad=bad), self.assertRaises(ShowRuleError):
                compile_neighbour(bad, field="f")

    def test_snapshot_round_trip(self):
        for spec in (True, {"min": 10}, {"max": 30}, {"min": 10, "max": 30}):
            neighbour = compile_neighbour(spec, field="f")
            self.assertEqual(neighbour, compile_neighbour(neighbour_snapshot(neighbour), field="f"))
        self.assertEqual({}, neighbour_snapshot(Neighbour()))
        self.assertIsNone(neighbour_snapshot(None))


class AlternativeTests(unittest.TestCase):
    """Operators with several alternatives behave like one operator per alternative."""

    def test_every_operator_accepts_a_list_of_alternatives(self):
        cases = [
            ("equals", ["Alpha", "Beta"], "beta", "Gamma"),
            ("starts_with", ["Al", "Be"], "Beta show", "Gamma show"),
            ("ends_with", ["pha", "eta"], "Alpha", "Gamma"),
            ("contains", ["lph", "bet"], "a BETA b", "Gamma"),
            ("wildcard", ["Al*", "?eta"], "Beta", "Gamma"),
            ("regex", ["^Al", "eta$"], "Beta", "Gamma"),
        ]
        for operator, values, hit, miss in cases:
            with self.subTest(operator=operator):
                self.assertTrue(matches({"title": {operator: values}}, title=hit))
                self.assertFalse(matches({"title": {operator: values}}, title=miss))

    def test_combinators_short_circuit_correctly(self):
        spec = {"title": {"any": [{"contains": "a"}, {"all": [{"contains": "b"}, {"not": {"contains": "c"}}]}]}}
        self.assertTrue(matches(spec, title="xa"))
        self.assertTrue(matches(spec, title="xb"))
        self.assertFalse(matches(spec, title="xbc"))
        self.assertFalse(matches(spec, title="x"))

    def test_fields_still_combine_with_and(self):
        spec = {"title": {"contains": "a"}, "description": {"contains": "b"}}
        self.assertTrue(matches(spec, title="a", description="b"))
        self.assertFalse(matches(spec, title="a", description="c"))
        self.assertFalse(matches(spec, title="c", description="b"))

    def test_sharp_s_and_decomposed_letters_match_their_folded_forms(self):
        self.assertTrue(matches({"title": {"contains": "strasse"}}, title="Hauptstraße"))
        self.assertTrue(matches({"title": {"contains": "für"}}, title="Fu\u0308r Elise"))
        self.assertTrue(matches({"title": {"equals": "a b"}}, title="a\u00a0b"))
        self.assertFalse(matches({"title": {"contains": "strasse"}, "case_sensitive": True}, title="Hauptstraße"))


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
