import logging
import unittest
from unittest.mock import patch

from catchuparr import logging_utils
from catchuparr.logging_utils import apply_log_level, error, event, normalize_log_level


class LoggingUtilsTests(unittest.TestCase):
    def tearDown(self):
        logging_utils._error_windows.clear()
        logging.getLogger("catchuparr").setLevel(logging.NOTSET)

    def test_level_validation_and_scoped_application(self):
        root = logging.getLogger()
        original_root_level = root.level
        self.assertEqual("DEBUG", normalize_log_level("debug"))
        with self.assertRaisesRegex(ValueError, "log_level"):
            normalize_log_level("NOTSET")
        self.assertEqual("WARNING", apply_log_level("warning"))
        self.assertEqual(logging.WARNING, logging.getLogger("catchuparr").level)
        self.assertEqual(original_root_level, root.level)
        with self.assertLogs("catchuparr", level="WARNING") as captured:
            self.assertEqual("INFO", apply_log_level("TRACE"))
            self.assertEqual(logging.INFO, logging.getLogger("catchuparr").level)
        self.assertIn("[Catchuparr] setting_invalid reason=log_level", captured.output[0])

    def test_error_categories_remain_visible_at_error_level(self):
        apply_log_level("ERROR")
        with self.assertLogs("catchuparr", level="ERROR") as captured:
            error("supervision_failed")
        self.assertEqual("[Catchuparr] supervision_failed", captured.records[0].getMessage())

    def test_events_are_prefixed_and_drop_unapproved_values(self):
        with self.assertLogs("catchuparr", level="INFO") as captured:
            event(
                "control_paused",
                paused=True,
                control_generation=7,
                reason="SyntheticProvider",
                url="https://synthetic.invalid/secret",
            )
        self.assertEqual(1, len(captured.records))
        self.assertIn("[Catchuparr]", captured.output[0])
        self.assertIn("paused=true", captured.output[0])
        self.assertIn("control_generation=7", captured.output[0])
        self.assertNotIn("SyntheticProvider", captured.output[0])
        self.assertNotIn("synthetic.invalid", captured.output[0])

    def test_repeated_errors_are_summarized_after_a_bounded_window(self):
        with patch("catchuparr.logging_utils.time.monotonic", side_effect=[0.0, 1.0, 301.0]):
            with self.assertLogs("catchuparr", level="WARNING") as captured:
                error("supervision_failed")
                error("supervision_failed")
                error("supervision_failed")
        self.assertEqual(3, len(captured.records))
        self.assertEqual("[Catchuparr] supervision_failed", captured.records[0].getMessage())
        self.assertEqual(
            "[Catchuparr] error_suppressed category=supervision_failed count=1",
            captured.records[-2].getMessage(),
        )
        self.assertEqual("[Catchuparr] supervision_failed", captured.records[-1].getMessage())
        self.assertLessEqual(len(logging_utils._error_windows), logging_utils.MAX_ERROR_CATEGORIES)


if __name__ == "__main__":
    unittest.main()
