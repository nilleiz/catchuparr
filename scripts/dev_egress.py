"""Install/remove a narrow DOCKER-USER egress rule for the Dev bridge.

Run as root on the Docker host. The Dev bridge must exist before applying the
rule, and the runtime stack must not be attached when changing/removing it.
No production container, network or firewall rule is modified by name.
"""

from __future__ import annotations

import argparse
import ipaddress
import json
import subprocess

CHAIN = "CATCHUPARR_DEV_EGRESS"
NETWORK = "catchuparr-dev-lan"


def command(*args: str, check: bool = True) -> subprocess.CompletedProcess[str]:
    return subprocess.run(args, text=True, capture_output=True, check=check)


def network_state() -> dict:
    return json.loads(command("docker", "network", "inspect", NETWORK).stdout)[0]


def rules(subnet: str, target: str, port: int) -> list[list[str]]:
    return [
        ["-m", "conntrack", "--ctstate", "ESTABLISHED,RELATED", "-j", "RETURN"],
        ["-d", f"{target}/32", "-p", "tcp", "--dport", str(port), "-j", "RETURN"],
        ["-j", "REJECT"],
    ]


def assert_network(subnet: str, *, empty: bool = False) -> None:
    network = network_state()
    actual = {part.get("Subnet") for part in network["IPAM"]["Config"]}
    if actual != {subnet} or network.get("EnableIPv6"):
        raise RuntimeError("Dev bridge subnet or IPv6 configuration differs from the approved rule")
    if empty and network.get("Containers"):
        raise RuntimeError("Stop the Dev runtime containers before changing the firewall rule")


def installed_rules() -> list[str] | None:
    result = command("iptables", "-w", "-S", CHAIN, check=False)
    if result.returncode:
        return None
    return result.stdout.splitlines()[1:]


def matches_rules(subnet: str, target: str, port: int) -> bool:
    installed = installed_rules()
    if installed is None or len(installed) != 3:
        return False
    if "conntrack" not in installed[0] or target not in installed[1] or not installed[2].endswith("-j REJECT"):
        return False
    return all(
        command("iptables", "-w", "-C", CHAIN, *rule, check=False).returncode == 0
        for rule in rules(subnet, target, port)
    )


def jump_exists(subnet: str) -> bool:
    return command(
        "iptables", "-w", "-C", "DOCKER-USER", "-s", subnet, "-j", CHAIN,
        check=False,
    ).returncode == 0


def apply(subnet: str, target: str, port: int) -> None:
    assert_network(subnet)
    existing = installed_rules()
    if existing is not None and not matches_rules(subnet, target, port):
        raise RuntimeError("Existing Dev egress chain differs; stop runtime and remove it first")
    if existing is None:
        command("iptables", "-w", "-N", CHAIN)
        for rule in rules(subnet, target, port):
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
    if args.action != "remove" and target is None:
        parser.error("--vu-ip is required for apply/check")
    if not 1 <= args.vu_port <= 65535:
        parser.error("--vu-port must be a TCP port")
    if args.action == "apply":
        apply(subnet, target, args.vu_port)
    elif args.action == "remove":
        remove(subnet)
    else:
        assert_network(subnet)
        if not matches_rules(subnet, target, args.vu_port) or not jump_exists(subnet):
            raise RuntimeError("Dev egress restriction is not installed as expected")


if __name__ == "__main__":
    main()
