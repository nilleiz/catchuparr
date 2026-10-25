import unittest

from catchuparr.catchup_request import MAX_ARCHIVE_SECONDS, archive_request_end


class GenericCatchupRangeTests(unittest.TestCase):
    def test_partial_substitution_uses_guide_end_not_client_clock(self):
        for duration in ("", "{duration}", "${duration}"):
            with self.subTest(duration=duration):
                self.assertEqual(1600, archive_request_end(
                    {"duration": duration, "lutc": "2000"}, 1000, 2100,
                    programme_end=1600,
                ))

    def test_guide_less_request_uses_bounded_snapshot_and_clamps_client_future(self):
        self.assertEqual(1800, archive_request_end({"lutc": "1800"}, 1000, 2000))
        self.assertEqual(2000, archive_request_end({"lutc": "3000"}, 1000, 2000))
        self.assertEqual(2000, archive_request_end({}, 1000, 2000))
        self.assertEqual(1000 + MAX_ARCHIVE_SECONDS,
                         archive_request_end({}, 1000, 200000))

    def test_numeric_seconds_and_explicit_end_take_priority_over_epg(self):
        self.assertEqual(1060, archive_request_end({"duration": "60"}, 1000, 2000,
                                                 programme_end=1800))
        self.assertEqual(1060, archive_request_end({"utcend": "1060"}, 1000, 2000))
        self.assertEqual(1060, archive_request_end(
            {"duration": "60", "utcend": "1060"}, 1000, 2000,
        ))

    def test_invalid_or_conflicting_bounds_stay_denied(self):
        for query in ({"duration": "0"}, {"duration": "-1"}, {"duration": "1.5"},
                      {"duration": "garbage"}, {"duration": "%7Bduration%7D"},
                      {"duration": "86401"}, {"utcend": "900"},
                      {"lutc": "900"}, {"lutc": "nan"},
                      {"duration": "60", "utcend": "1100"}):
            with self.subTest(query=query), self.assertRaises(ValueError):
                archive_request_end(query, 1000, 2000)

    def test_duplicate_query_keys_do_not_silently_override_requested_time(self):
        class Query(dict):
            def getlist(self, key):
                return ["1000", "2000"] if key == "utc" else []

        with self.assertRaises(ValueError):
            archive_request_end(Query(), 1000, 2000)


if __name__ == "__main__":
    unittest.main()
