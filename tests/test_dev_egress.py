import subprocess
import sys
import unittest
from unittest.mock import patch

from scripts import dev_egress


class EgressRulesTests(unittest.TestCase):
    def test_deny_all_rules_allow_established_replies_then_reject(self):
        self.assertEqual(
            dev_egress.rules(None, 8001),
            [
                ["-m", "conntrack", "--ctstate", "ESTABLISHED,RELATED", "-j", "RETURN"],
                ["-j", "REJECT"],
            ],
        )

    def test_vu_allowlist_is_between_established_replies_and_reject(self):
        self.assertEqual(
            dev_egress.rules("192.0.2.20", 8080),
            [
                ["-m", "conntrack", "--ctstate", "ESTABLISHED,RELATED", "-j", "RETURN"],
                ["-d", "192.0.2.20/32", "-p", "tcp", "--dport", "8080", "-j", "RETURN"],
                ["-j", "REJECT"],
            ],
        )

    def test_check_requires_the_exact_rule_sequence(self):
        canonical = [
            ["-m", "conntrack", "--ctstate", "ESTABLISHED,RELATED", "-j", "RETURN"],
            ["-d", "192.0.2.20/32", "-p", "tcp", "--dport", "8080", "-j", "RETURN"],
            ["-j", "REJECT"],
        ]
        with patch.object(dev_egress, "installed_rules", return_value=canonical):
            self.assertTrue(dev_egress.matches_rules("192.0.2.20", 8080))
            self.assertFalse(dev_egress.matches_rules(None, 8001))
        with patch.object(dev_egress, "installed_rules", return_value=[canonical[2], *canonical[:2]]):
            self.assertFalse(dev_egress.matches_rules("192.0.2.20", 8080))
        with patch.object(dev_egress, "installed_rules", return_value=[*canonical, canonical[2]]):
            self.assertFalse(dev_egress.matches_rules("192.0.2.20", 8080))

    def test_cli_applies_deny_all_without_vu_ip(self):
        with (
            patch.object(sys, "argv", ["dev_egress.py", "apply", "--subnet", "192.0.2.0/24"]),
            patch.object(dev_egress, "apply") as apply,
        ):
            dev_egress.main()
        apply.assert_called_once_with("192.0.2.0/24", None, 8001)

    def test_cli_applies_vu_allowlist_with_the_requested_port(self):
        with (
            patch.object(
                sys,
                "argv",
                [
                    "dev_egress.py", "apply", "--subnet", "192.0.2.0/24",
                    "--vu-ip", "192.0.2.20", "--vu-port", "8080",
                ],
            ),
            patch.object(dev_egress, "apply") as apply,
        ):
            dev_egress.main()
        apply.assert_called_once_with("192.0.2.0/24", "192.0.2.20", 8080)

    def test_cli_checks_deny_all_without_vu_ip(self):
        with (
            patch.object(sys, "argv", ["dev_egress.py", "check", "--subnet", "192.0.2.0/24"]),
            patch.object(dev_egress, "assert_network") as assert_network,
            patch.object(dev_egress, "matches_rules", return_value=True) as matches_rules,
            patch.object(dev_egress, "jump_exists", return_value=True) as jump_exists,
        ):
            dev_egress.main()
        assert_network.assert_called_once_with("192.0.2.0/24")
        matches_rules.assert_called_once_with(None, 8001)
        jump_exists.assert_called_once_with("192.0.2.0/24")

    def test_cli_rejects_a_port_without_vu_ip(self):
        with (
            patch.object(
                sys,
                "argv",
                ["dev_egress.py", "apply", "--subnet", "192.0.2.0/24", "--vu-port", "8080"],
            ),
            patch.object(dev_egress, "apply") as apply,
            self.assertRaises(SystemExit),
        ):
            dev_egress.main()
        apply.assert_not_called()


class InstalledRulesTests(unittest.TestCase):
    def test_installed_rules_are_parsed_and_conntrack_states_normalized(self):
        output = """-N CATCHUPARR_DEV_EGRESS
-A CATCHUPARR_DEV_EGRESS -m conntrack --ctstate RELATED,ESTABLISHED -j RETURN
-A CATCHUPARR_DEV_EGRESS -j REJECT
"""
        completed = subprocess.CompletedProcess([], 0, stdout=output, stderr="")
        with patch.object(dev_egress, "command", return_value=completed):
            self.assertEqual(
                dev_egress.installed_rules(),
                [
                    ["-m", "conntrack", "--ctstate", "ESTABLISHED,RELATED", "-j", "RETURN"],
                    ["-j", "REJECT"],
                ],
            )


if __name__ == "__main__":
    unittest.main()
