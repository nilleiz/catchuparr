"""Install/remove a narrow DOCKER-USER egress rule for the Dev bridge.

Run as root on the Docker host. The Dev bridge must exist before applying the
rule, and the runtime stack must not be attached when changing/removing it.
No production container, network or firewall rule is modified by name.
"""

from __future__ import annotations

import argparse
import ipaddress
import json
import shlex
import subprocess

CHAIN = "CATCHUPARR_DEV_EGRESS"
NETWORK = "catchuparr-dev-lan"


def command(*args: str, check: bool = True) -> subprocess.CompletedProcess[str]:
    return subprocess.run(args, text=True, capture_output=True, check=check)


def network_state() -> dict:
    return json.loads(command("docker", "network", "inspect", NETWORK).stdout)[0]


def rules(target: str | None, port: int) -> list[list[str]]:
    result = [["-m", "conntrack", "--ctstate", "ESTABLISHED,RELATED", "-j", "RETURN"]]
    if target is not None:
        result.append(["-d", f"{target}/32", "-p", "tcp", "--dport", str(port), "-j", "RETURN"])
    result.append(["-j", "REJECT"])
    return result


def assert_network(subnet: str, *, empty: bool = False) -> None:
    network = network_state()
    actual = {part.get("Subnet") for part in network["IPAM"]["Config"]}
    if actual != {subnet} or network.get("EnableIPv6"):
        raise RuntimeError("Dev bridge subnet or IPv6 configuration differs from the approved rule")
    if empty and network.get("Containers"):
        raise RuntimeError("Stop the Dev runtime containers before changing the firewall rule")


def _canonical_rule(rule: list[str]) -> list[str]:
    """Normalize iptables' unordered conntrack state list for exact comparison."""
    result = list(rule)
    if "--ctstate" in result:
        index = result.index("--ctstate") + 1
        result[index] = ",".join(sorted(result[index].split(",")))
    return result


def installed_rules() -> list[list[str]] | None:
    result = command("iptables", "-w", "-S", CHAIN, check=False)
    if result.returncode:
        return None
    lines = [shlex.split(line) for line in result.stdout.splitlines()[1:]]
    if any(len(line) < 3 or line[:2] != ["-A", CHAIN] for line in lines):
        return None
    return [_canonical_rule(line[2:]) for line in lines]


def matches_rules(target: str | None, port: int) -> bool:
    installed = installed_rules()
    expected = [_canonical_rule(rule) for rule in rules(target, port)]
    return installed == expected


def jump_exists(subnet: str) -> bool:
    return command(
        "iptables", "-w", "-C", "DOCKER-USER", "-s", subnet, "-j", CHAIN,
        check=False,
    ).returncode == 0


def apply(subnet: str, target: str | None, port: int) -> None:
    assert_network(subnet)
    existing = installed_rules()
    if existing is not None and not matches_rules(target, port):
        raise RuntimeError("Existing Dev egress chain differs; stop runtime and remove it first")
    if existing is None:
        command("iptables", "-w", "-N", CHAIN)
        for rule in rules(target, port):
            command("iptables", "-w", "-A", CHAIN, *rule)
    if not jump_exists(subnet):
        command("iptables", "-w", "-I", "DOCKER-USER", "1", "-s", subnet, "-j", CHAIN)


def remove(subnet: str) -> None:
    assert_network(subnet, empty=True)
    if jump_exists(subnet):
        command("iptables", "-w", "-D", "DOCKER-USER", "-s", subnet, "-j", CHAIN)
    if installed_rules() is not None:
        command("iptables", "-w", "-F", CHAIN)
        command("iptables", "-w", "-X", CHAIN)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=("apply", "check", "remove"))
    parser.add_argument("--subnet", required=True)
    parser.add_argument("--vu-ip")
    parser.add_argument("--vu-port", type=int, default=8001)
    args = parser.parse_args()
    subnet = str(ipaddress.ip_network(args.subnet, strict=True))
    if not isinstance(ipaddress.ip_network(subnet), ipaddress.IPv4Network):
        parser.error("the Dev bridge must use IPv4")
    target = str(ipaddress.IPv4Address(args.vu_ip)) if args.vu_ip else None
    if not 1 <= args.vu_port <= 65535:
        parser.error("--vu-port must be a TCP port")
    if target is None and args.vu_port != 8001:
        parser.error("--vu-port requires --vu-ip")
    if args.action == "apply":
        apply(subnet, target, args.vu_port)
    elif args.action == "remove":
        remove(subnet)
    else:
        assert_network(subnet)
        if not matches_rules(target, args.vu_port) or not jump_exists(subnet):
            raise RuntimeError("Dev egress restriction is not installed as expected")


if __name__ == "__main__":
    main()
