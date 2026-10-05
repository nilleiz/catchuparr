import unittest
from types import SimpleNamespace

from catchuparr.views import _catchup_epoch, _core_request


class ViewBoundaryTests(unittest.TestCase):
    def test_utc_epoch_and_iso_timestamp_agree(self):
        epoch = _catchup_epoch("2026-10-05T12:00:00+02:00")
        self.assertEqual(epoch, _catchup_epoch(str(int(epoch))))

    def test_rejects_ambiguous_and_nonfinite_time(self):
        for timestamp in ("2026-10-05T12:00:00", "nan", "inf", ""):
            with self.subTest(timestamp=timestamp), self.assertRaises(ValueError):
                _catchup_epoch(timestamp)

    def test_core_request_excludes_bearer_values_from_cache_key(self):
        request = SimpleNamespace(GET={"access_token": "secret", "days": "2"})
        core = _core_request(request)
        self.assertEqual(core.GET, {"days": "2"})
        self.assertIn("access_token", request.GET)


if __name__ == "__main__":
    unittest.main()
