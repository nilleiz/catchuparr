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
    """Normalize iptables' equivalent renderings for exact rule comparison."""
    result = list(rule)
    if "--ctstate" in result:
        index = result.index("--ctstate") + 1
        result[index] = ",".join(sorted(result[index].split(",")))
    if "-p" in result and result[result.index("-p") + 1] == "tcp":
        compact = []
        index = 0
        while index < len(result):
            if result[index:index + 2] == ["-m", "tcp"]:
                index += 2
                continue
            compact.append(result[index])
            index += 1
        result = compact
    if "-j" in result and result[result.index("-j") + 1] == "REJECT":
        try:
            reject_with = result.index("--reject-with")
        except ValueError:
            pass
        else:
            if result[reject_with + 1] == "icmp-port-unreachable":
                del result[reject_with:reject_with + 2]
    return result


def chain_rules(chain: str) -> list[list[str]] | None:
    result = command("iptables", "-w", "-S", chain, check=False)
    if result.returncode:
        return None
    lines = [shlex.split(line) for line in result.stdout.splitlines()]
    if lines and lines[0][:2] in (["-N", chain], ["-P", chain]):
        lines = lines[1:]
    if any(len(line) < 3 or line[:2] != ["-A", chain] for line in lines):
        return None
    return [_canonical_rule(line[2:]) for line in lines]


def installed_rules() -> list[list[str]] | None:
    return chain_rules(CHAIN)


def matches_rules(target: str | None, port: int) -> bool:
    installed = installed_rules()
    expected = [_canonical_rule(rule) for rule in rules(target, port)]
    return installed == expected


def _dev_jump(subnet: str) -> list[str]:
    return ["-s", subnet, "-j", CHAIN]


def jump_is_first(subnet: str) -> bool:
    installed = chain_rules("DOCKER-USER")
    expected = _dev_jump(subnet)
    if installed is None or not installed or installed[0] != expected:
        return False
    targets = [
        rule for rule in installed
        if ("-j" in rule and rule[rule.index("-j") + 1] == CHAIN)
        or ("-g" in rule and rule[rule.index("-g") + 1] == CHAIN)
    ]
    return targets == [expected]


def ensure_jump_first(subnet: str) -> None:
    installed = chain_rules("DOCKER-USER")
    if installed is None:
        raise RuntimeError("Unable to inspect DOCKER-USER chain")
    expected = _dev_jump(subnet)
    positions = [index for index, rule in enumerate(installed) if rule == expected]
    for rule in installed:
        if ("-j" in rule and rule[rule.index("-j") + 1] == CHAIN) or (
            "-g" in rule and rule[rule.index("-g") + 1] == CHAIN
        ):
            if rule != expected:
                raise RuntimeError("Unexpected Dev egress jump in DOCKER-USER chain")
    if positions == [0]:
        return
    for _ in positions:
        command("iptables", "-w", "-D", "DOCKER-USER", *expected)
    command("iptables", "-w", "-I", "DOCKER-USER", "1", *expected)
    if not jump_is_first(subnet):
        raise RuntimeError("Failed to place Dev egress restriction first in DOCKER-USER")


def apply(subnet: str, target: str | None, port: int) -> None:
    assert_network(subnet)
    existing = installed_rules()
    if existing is not None and not matches_rules(target, port):
        raise RuntimeError("Existing Dev egress chain differs; stop runtime and remove it first")
    if existing is None:
        command("iptables", "-w", "-N", CHAIN)
        for rule in rules(target, port):
            command("iptables", "-w", "-A", CHAIN, *rule)
    ensure_jump_first(subnet)


def remove(subnet: str) -> None:
    assert_network(subnet, empty=True)
    installed = chain_rules("DOCKER-USER")
    if installed is None:
        raise RuntimeError("Unable to inspect DOCKER-USER chain")
    expected = _dev_jump(subnet)
    if any(
        (("-j" in rule and rule[rule.index("-j") + 1] == CHAIN)
         or ("-g" in rule and rule[rule.index("-g") + 1] == CHAIN))
        and rule != expected for rule in installed
    ):
        raise RuntimeError("Unexpected Dev egress jump in DOCKER-USER chain")
    for rule in installed:
        if rule == expected:
            command("iptables", "-w", "-D", "DOCKER-USER", *expected)
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
        if not matches_rules(target, args.vu_port) or not jump_is_first(subnet):
            raise RuntimeError("Dev egress restriction is not installed as expected")


if __name__ == "__main__":
    main()
