"""Exercise the 0.5.1 configuration compiler against real Django catalog rows."""

import json
import sys
import tempfile
import uuid
from pathlib import Path


def require(condition, message):
    if not condition:
        raise RuntimeError(message)


_ACTIVE_SCHEMA_VERSION = 6
_SHARING_DEFAULTS = {
    "enabled": False,
    "live_stability_seconds": 5,
    "last_viewer_return_seconds": 30,
    "min_switch_interval_seconds": 60,
}


def validate_active_v6_snapshot(active, persisted, channel_uuids):
    """Keep this compatibility probe tied to the current persisted schema."""
    expected_channels = set(channel_uuids)
    require(
        isinstance(active, dict) and active.get("version") == _ACTIVE_SCHEMA_VERSION,
        "Apply did not persist a version 6 active snapshot",
    )
    require(
        isinstance(persisted, dict) and persisted.get("version") == _ACTIVE_SCHEMA_VERSION,
        "raw active snapshot did not persist schema version 6",
    )
    sharing = active.get("connection_sharing")
    require(
        isinstance(sharing, dict) and set(sharing) == expected_channels,
        "v6 active snapshot must resolve sharing defaults for every selected channel",
    )
    require(
        all(value == _SHARING_DEFAULTS for value in sharing.values()),
        "blank sharing configuration must keep every channel disabled with default cooldowns",
    )
    require(
        persisted.get("connection_sharing") == sharing,
        "raw v6 sharing snapshot differs from the active configuration view",
    )


def probe():
    sys.path.insert(0, "/data/plugins")
    from apps.channels.models import (
        Channel,
        ChannelProfile,
        ChannelProfileMembership,
        ChannelStream,
        Stream,
    )
    from apps.m3u.models import M3UAccount

    from catchuparr.configuration import (
        ACTIVE_CONFIG_VERSION,
        apply_configuration,
        load_active_configuration,
        source_catalog,
        validate_configuration,
    )
    from catchuparr.source_rules import SourceRuleError, compile_filter_config

    suffix = uuid.uuid4().hex[:10]
    created_accounts = []
    created_streams = []
    created_channels = []
    created_profiles = []
    try:
        account_a = M3UAccount.objects.create(
            name=f"Synthetic 051 Source A {suffix}", max_streams=2,
        )
        account_b = M3UAccount.objects.create(
            name=f"Synthetic 051 Source B {suffix}", max_streams=2,
        )
        account_c = M3UAccount.objects.create(
            name=f"Synthetic 051 Source C {suffix}", max_streams=2,
        )
        created_accounts.extend((account_a, account_b, account_c))

        source_a = Stream.objects.create(
            name=f"Synthetic 051 Stream A {suffix}",
            url="http://127.0.0.1:1/synthetic-a.ts", m3u_account=account_a,
        )
        source_b = Stream.objects.create(
            name=f"Synthetic 051 Stream B {suffix}",
            url="http://127.0.0.1:1/synthetic-b.ts", m3u_account=account_b,
        )
        created_streams.extend((source_a, source_b))
        channel_a = Channel.objects.create(
            name=f"Synthetic 051 Channel A {suffix}", channel_number=9501, user_level=0,
        )
        channel_b = Channel.objects.create(
            name=f"Synthetic 051 Channel B {suffix}", channel_number=9502, user_level=0,
        )
        created_channels.extend((channel_a, channel_b))
        ChannelStream.objects.create(channel=channel_a, stream=source_a, order=0)
        ChannelStream.objects.create(channel=channel_a, stream=source_b, order=1)
        ChannelStream.objects.create(channel=channel_b, stream=source_a, order=0)
        ChannelStream.objects.create(channel=channel_b, stream=source_b, order=1)
        profile_name = f"Synthetic 051 Profile {suffix}"
        profile = ChannelProfile.objects.create(name=profile_name)
        created_profiles.append(profile)
        for channel in (channel_a, channel_b):
            ChannelProfileMembership.objects.update_or_create(
                channel_profile=profile, channel=channel, defaults={"enabled": True},
            )

        catalog = source_catalog()
        require(
            {str(channel_a.uuid), str(channel_b.uuid)}
            <= {row["uuid"] for row in catalog.channels},
            "real catalog omitted one or more fixture channels",
        )
        require(
            len(catalog.streams_by_channel[str(channel_a.uuid)]) == 2,
            "real catalog did not expose both assigned synthetic streams",
        )

        base_rule = (
            f"  - channels: {{profile: {profile_name}}}\n"
            f"    include: [{account_a.name}]\n"
            f"    priority: {{{account_a.name}: 10}}\n"
            "    schedule:\n"
            "      daily: [{start: '07:00', end: '08:00'}]\n"
            "      weekdays: [{start: '18:00', end: '20:00'}]\n"
            "      monday: [{start: '23:00', end: '02:00'}]\n"
            "      tuesday: [{start: '06:00', end: '07:00'}]\n"
            "      sunday: []\n"
        )
        direct_by_number = (
            f"  - channels: {{numbers: [9501]}}\n"
            f"    exclude: [{account_a.name}]\n"
            f"    priority: {{{account_b.name}: 20}}\n"
            "    schedule_mode: override_days\n"
            "    schedule:\n"
            "      monday: []\n"
            "      tuesday: [{start: '09:00', end: '10:00'}]\n"
        )
        direct_by_name = direct_by_number.replace(
            "channels: {numbers: [9501]}",
            f"channels: {{names: [{channel_a.name}]}}",
        )
        document_prefix = f"version: 1\nprofile: {profile_name}\ntimezone: UTC\nrules:\n"
        number_text = document_prefix + direct_by_number + base_rule
        name_text = document_prefix + base_rule + direct_by_name

        compiled_numbers = compile_filter_config(
            number_text, catalog.channels, catalog.accounts, catalog.profiles,
            catalog.streams_by_channel,
        )
        compiled_names = compile_filter_config(
            name_text, catalog.channels, catalog.accounts, catalog.profiles,
            catalog.streams_by_channel,
        )
        require(
            compiled_numbers.source_policies == compiled_names.source_policies
            and compiled_numbers.channel_schedules == compiled_names.channel_schedules,
            "direct name and number exceptions must be order independent",
        )
        require(
            compiled_numbers.source_policies[str(channel_a.uuid)].include_account_ids is None
            and compiled_numbers.source_policies[str(channel_a.uuid)].exclude_account_ids
            == frozenset({str(account_a.id)})
            and compiled_numbers.source_policies[str(channel_a.uuid)].priorities
            == ((str(account_b.id), 20),),
            "later direct exception did not replace the inherited filter and priority map",
        )
        require(
            compiled_numbers.source_policies[str(channel_b.uuid)].include_account_ids
            == frozenset({str(account_a.id)})
            and compiled_numbers.source_policies[str(channel_b.uuid)].priorities
            == ((str(account_a.id), 10),),
            "named-profile member did not inherit the base source policy",
        )

        expected_base_schedule = {
            "mode": "weekly",
            "intervals": [[1380, 1560], [1800, 1860], [3960, 4080],
                          [5400, 5520], [6840, 6960], [7620, 7680]],
        }
        expected_direct_schedule = {
            "mode": "weekly",
            "intervals": [[1980, 2040], [3960, 4080], [5400, 5520],
                          [6840, 6960], [7620, 7680]],
        }
        require(
            compiled_numbers.channel_schedules[str(channel_b.uuid)].to_snapshot()
            == expected_base_schedule,
            "replace schedule must turn unspecified weekdays off and apply daily/group/day precedence",
        )
        require(
            compiled_numbers.channel_schedules[str(channel_a.uuid)].to_snapshot()
            == expected_direct_schedule,
            "empty override day must remove inherited overnight windows owned by that start day",
        )

        invalid_final = number_text.replace(
            f"    priority: {{{account_b.name}: 20}}\n", "", 1,
        )
        try:
            compile_filter_config(
                invalid_final, catalog.channels, catalog.accounts, catalog.profiles,
                catalog.streams_by_channel,
            )
        except SourceRuleError as error:
            require("excluded by source filter" in str(error), "wrong invalid-policy diagnostic")
        else:
            raise RuntimeError("unreplaced inherited priority must be rejected after final merge")

        replace_only = document_prefix + (
            f"  - channels: {{profile: {profile_name}}}\n"
            "    schedule:\n"
            "      monday: [{start: '08:00', end: '09:00'}]\n"
        )
        replacement = compile_filter_config(
            replace_only, catalog.channels, catalog.accounts, catalog.profiles,
            catalog.streams_by_channel,
        )
        require(
            replacement.channel_schedules[str(channel_a.uuid)].to_snapshot()
            == {"mode": "weekly", "intervals": [[480, 540]]},
            "schedule replacement must switch unspecified days off",
        )

        settings = {
            "filter_config": number_text,
            "archive_root": "",
            "retention_hours": 1,
            "max_storage_gib": 1,
            "recording_enabled": False,
        }
        with tempfile.TemporaryDirectory(prefix="catchuparr-051-native-") as temporary:
            root = Path(temporary)
            settings["archive_root"] = str(root / "archive")
            active_path = root / ".catchuparr-active-settings.json"
            validated = validate_configuration(settings)
            applied = apply_configuration(settings, active_path=active_path)
            active = load_active_configuration(active_path)
            persisted = json.loads(active_path.read_text(encoding="utf-8"))
            require(applied.get("applied") is True, "real Django configuration Apply failed")
            require(validated["channels"] == applied["channels"],
                    "Validate and Apply previews diverged")
            require(ACTIVE_CONFIG_VERSION == _ACTIVE_SCHEMA_VERSION,
                    "configuration probe must be updated when the active schema changes")
            validate_active_v6_snapshot(
                active, persisted, (str(channel_a.uuid), str(channel_b.uuid)),
            )
            require(active["recording_schedule"]["channels"] == {
                str(channel_a.uuid): expected_direct_schedule,
                str(channel_b.uuid): expected_base_schedule,
            }, "v6 active snapshot did not retain effective schedules")
            require(active["source_policies"][str(channel_a.uuid)]["priorities"]
                    == [[str(account_b.id), 20]], "v6 snapshot did not retain replacement priorities")
            previews = {row["channel_uuid"]: row for row in validated["channels"]}
            require(persisted["configuration_origins"] == {
                key: value["configuration_origin"] for key, value in previews.items()
            }, "raw v6 persisted origins differ from the validated effective preview")
            require(previews[str(channel_a.uuid)]["effective_source_policy"] == {
                "include": None, "exclude": [account_a.name],
                "priority": {account_b.name: 20},
            }, "preview omitted canonical effective policy")
            require(previews[str(channel_a.uuid)]["configuration_origin"]["source_filter"] == "rule 1"
                    and previews[str(channel_a.uuid)]["configuration_origin"]["priority"] == "rule 1",
                    "preview omitted direct exception origins")
            require(previews[str(channel_b.uuid)]["configuration_origin"]["source_filter"] == "rule 2"
                    and previews[str(channel_b.uuid)]["configuration_origin"]["priority"] == "rule 2",
                    "preview omitted profile inheritance origins")

        print(json.dumps({
            "status": "pass",
            "cases": [
                "real Django ORM source catalog fixtures",
                "named profile with number and name exceptions independent of YAML order",
                "whole-map filter and priority replacement plus final invalid-policy rejection",
                "Validate/Apply effective preview, origins, and v6 snapshot parity",
                "whole-week replacement, group/day precedence, explicit empty days, overnight start-day ownership",
            ],
            "provider_connections": 0,
        }, sort_keys=True))
    finally:
        for channel in created_channels:
            ChannelStream.objects.filter(channel=channel).delete()
            ChannelProfileMembership.objects.filter(channel=channel).delete()
        Channel.objects.filter(id__in=[item.id for item in created_channels]).delete()
        ChannelProfile.objects.filter(id__in=[item.id for item in created_profiles]).delete()
        Stream.objects.filter(id__in=[item.id for item in created_streams]).delete()
        M3UAccount.objects.filter(id__in=[item.id for item in created_accounts]).delete()


if __name__ == "__main__":
    probe()
