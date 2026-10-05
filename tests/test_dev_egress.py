import unittest

from scripts.dev_egress import rules


class DevEgressRulesTests(unittest.TestCase):
    def test_bootstrap_denies_new_connections(self):
        entries = rules("10.243.156.0/24", None, 8001)
        self.assertEqual(len(entries), 2)
        self.assertEqual(entries[0][-2:], ["-j", "RETURN"])
        self.assertEqual(entries[-1], ["-j", "REJECT"])

    def test_approved_source_is_the_only_new_connection_exception(self):
        entries = rules("10.243.156.0/24", "192.168.0.25", 8001)
        self.assertEqual(len(entries), 3)
        self.assertEqual(entries[1], [
            "-d", "192.168.0.25/32", "-p", "tcp", "--dport", "8001", "-j", "RETURN",
        ])
        self.assertEqual(entries[-1], ["-j", "REJECT"])
