# 0.3.1 candidate

This is an unreleased candidate for version `0.3.1`. It combines the existing
YAML source filters and recording schedules with day-group shorthand, applied
timezone resolution, one Apply operation for all settings, immediate Pause and
Resume controls, and labeled M3U/XMLTV token links. Independent review, required
CI, isolated Dev validation and real-player acceptance are release gates.

## Schedule and timezone

The filter document accepts explicit weekday keys and the `daily`, `weekdays`
and `weekend` groups. A more-specific entry replaces that day's entire window
list: explicit weekday, then its matching group, then `daily`. A day with no
resulting entry is off. A missing schedule means continuous recording; an empty
schedule means no recording.

```yaml
version: 1
timezone: Etc/UTC  # Optional. Overrides container/system timezone detection.
schedule:
  daily:
    - start: "07:00"
      end: "08:00"
    - start: "20:00"
      end: "22:00"
  weekdays:
    - start: "18:00"
      end: "22:00"
  weekend:
    - start: "10:00"
      end: "13:00"
    - start: "23:00"
      end: "01:00"
  monday:
    - start: "06:00"
      end: "09:00"
rules:
  - channels:
      numbers: [100]  # Synthetic channel selector.
    include: ["Synthetic M3U A", "Synthetic M3U B"]
    priority:
      "Synthetic M3U A": 100
      "Synthetic M3U B": 50
    schedule:
      weekdays:
        - start: "19:00"
          end: "22:00"
      friday:
        - start: "20:00"
          end: "23:00"
```

The global Monday list replaces `weekdays` for Monday; the remaining weekdays
use the group list. In the rule schedule, Friday replaces that rule's
`weekdays` list. Saturday and Sunday use `daily` because no `weekend` entry is
present in the rule schedule. A rule schedule replaces the global schedule for
the rule's channels. Adjacent and overlapping windows merge. An overnight
window belongs to its start weekday, and the end is exclusive. `24:00` is
allowed only as an end time; equal start and end values are invalid. DST gaps
produce no local instants, while repeated local times apply in both occurrences.

Resolve the timezone in this order: explicit YAML `timezone`, a valid container
`TZ`, then a detectable IANA timezone from the container's system timezone
configuration. Invalid configured values and an undetectable system timezone
fail validation. There is no fixed Berlin fallback for new configurations. The
resolved IANA timezone is stored with the applied schedule; later environment
changes take effect only after another successful Apply. The development
Compose file accepts an optional `TZ` pass-through and does not set a fixed
timezone by default.

The YAML parser rejects duplicate and unknown keys, unsupported types, tags,
anchors, aliases and merges. Errors identify the field and line without echoing
the full configuration document.

## Unified configuration Apply

Edit the complete settings draft, validate its preview, then choose **Apply
configuration** once. The same operation validates and activates filter YAML,
schedules, timezone, recording-enabled state, log level, archive path, retention,
storage limit, playback user and public token-link base URL. Saving draft fields
alone does not change recording. A validation or persistence failure must leave the prior applied
generation in force or deny recorder admission; it must not activate a partial
combination.

The optional **Public base URL for token links** setting supplies the
externally reachable http(s) origin, including an optional path prefix. It must
be applied before creating a token. The plugin does not infer a public origin
from an untrusted request host.

**Pause** and **Resume** remain immediate operational actions. They update the
recording-enabled setting and fence queued work without applying other draft
fields. Pausing recording does not disable archive playback, token access or
retention. Changing the archive root does not move or delete existing archive
data or access tokens; the Apply result identifies that the old data remains at
the previous path.

The active snapshot and recorder-control state are coordinated through the
plugin's shared activation lock and a fail-closed marker. The database draft
and filesystem snapshot are separate stores, so the implementation does not
claim a single database/filesystem transaction. On an interrupted or failed
activation, recorder admission remains denied until a valid Apply or recovery
completes.

## M3U and XMLTV access links

Set **Public base URL for token links**, apply the settings, then create an
access token for an existing Dispatcharr user. The confirmation
displays separate **M3U playlist URL** and **XMLTV EPG URL** lines. Both links
use the same token and that user's existing permissions. Copy the links from
the confirmation when needed; the token is shown only at creation. Never put a
token or authenticated URL in logs, support output or repository files.

## Logging

The **Catchuparr log level** setting defaults to `INFO` and accepts `DEBUG`,
`INFO`, `WARNING` or `ERROR`. It controls the Catchuparr logger across web and
worker processes using the applied configuration. Events use the
`[Catchuparr]` prefix with fixed event names and sanitized, allowlisted fields.
Repeated errors are bounded. Tokens, authenticated URLs, credentials and EPG
text are not logged.

## Build and validation status

Build the candidate with `python3 scripts/build_plugin.py`. Inspect the ZIP
contents and checksum before installation. Follow the isolated installation
and rollback instructions in [development and deployment](development.md).
The default restore point contains application/configuration data only; it
excludes the archive directory, recordings and archive database. Keep no more
than two verified task-owned Dev restore points. Include archive data only
with explicit authorization. A data-only restore point cannot recover archive
content that was lost or overwritten.

This guide describes candidate behavior; it is not a release notice. Required
CI, independent review, isolated Dev validation and real-player regression must
pass before publication.
