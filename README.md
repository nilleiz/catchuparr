# Catchuparr

<p align="center">
  <img src="docs/assets/catchuparr-icon-256.png" width="128" height="128" alt="Catchuparr logo">
</p>

Catchuparr adds a local rolling catch-up archive and start-over playback to
[Dispatcharr](https://github.com/Dispatcharr/Dispatcharr). It records selected
live channels into a shared local archive and exposes the indexed programmes
through authenticated M3U/XMLTV links. A native XC adapter is also available.

Dispatcharr remains responsible for live streams, EPG data, users and playback
permissions. Catchuparr does not create channel-to-provider assignments. The
archive is local to the Dispatcharr installation and is not a cloud service.

## Compatibility and status

- Version 0.4.0 is released. Version 0.5.0 implementation and user acceptance
  are complete; publication awaits the required release workflow and it is not
  yet published.
- Dispatcharr 0.31.0 and 0.32.0 are the supported versions covered by the
  repository's pinned native integration checks.
- The primary path is M3U/XMLTV with HLS playback using MPEG-TS segments.
- Native XC playback is implemented behind guarded Dispatcharr hooks. Native
  server integration is covered by synthetic checks; player-specific XC
  acceptance is still separate and should be verified with the client in use.
- The 0.5.0 native output integration adds an optional M3U/XMLTV archive
  toggle. The earlier implementation passed 433 tests and full pinned AIO probes
  on Dispatcharr 0.31.0 and 0.32.0; its final parity correction passed four
  focused native 0.32.0 cases. User acceptance is complete; required release CI
  remains pending. See the [0.5.0 guide](docs/candidate-0.5.0.md).
- Playback follows the selected user's Dispatcharr permissions, channel access,
  catch-up permission and stream/session limits. It does not provide transcoding
  or frame-accurate seeking.

See the [changelog](CHANGELOG.md), [validation record](docs/validation.md),
[development and deployment guide](docs/development.md), and
[release roadmap](docs/roadmap.md).

## Install or update

1. Download the `catchuparr-<version>.zip` asset from the
   [latest GitHub release](https://github.com/nilleiz/catchuparr/releases).
2. In Dispatcharr, open its plugin manager and import the ZIP. Use the plugin
   manager's overwrite/update option when replacing an existing Catchuparr.
3. Restart the Dispatcharr web and worker processes as required by the plugin
   manager so they all load the same package.
4. Configure the plugin as described below, then use **Validate** and
   **Apply configuration**.

The ZIP is a folder-shaped plugin package with `catchuparr/plugin.json` at its
root. Do not unpack it and upload an individual Python file.

When upgrading directly from a version before the 0.2.1 YAML configuration,
Catchuparr clears the obsolete channel selection. After upgrade, enter the new
YAML, validate it and Apply before expecting recording to resume. The reset
preserves archive data, recordings, general archive settings and playback
credentials.

## Configure the archive

Catchuparr's settings are plugin fields in Dispatcharr. Settings that affect
operation are activated together by **Apply configuration**. Editing or saving
a draft alone does not alter the active recorder configuration.

| Setting | Purpose |
| --- | --- |
| **Channel and source filter (YAML)** | Select channels, optionally restrict assigned M3U sources, rank allowed sources and set recording hours. Blank YAML selects no channels. |
| **Archive path** | Local directory for recordings and archive metadata. It must be writable and resolve to the same storage for every Dispatcharr web and Celery worker. |
| **Retention (hours)** | How far back archive playback is kept. Accepted range: 1–720 hours. |
| **Storage limit (GiB)** | Maximum total Catchuparr archive storage. Accepted range: 1–10240 GiB. |
| **User ID for new M3U token** | Existing Dispatcharr user ID to attach to a newly created playlist token. Set this before creating the token. |
| **Public base URL for token links** | Absolute externally reachable `http://` or `https://` base used in generated M3U and XMLTV links. It may include a path prefix, but cannot contain credentials, a query or a fragment. |
| **Recording enabled** | Whether scheduled recording is enabled. Apply the setting, or use the immediate Pause/Resume actions. |
| **Catchuparr log level** | Catchuparr-only log verbosity: `DEBUG`, `INFO`, `WARNING` or `ERROR`. |
| **Show archive playback in Stats** | Add active Catchuparr archive viewers to native Timeshift Stats. Enabled by default. |
| **Hide recorders in Stats** | Hide positively identified Catchuparr recorder clients from live Stats details. Enabled by default. |
| **Add Catchuparr catch-up to native output** | Optional 0.5.0 setting. Adds local archive metadata to eligible native Dispatcharr M3U/XMLTV output. Disabled by default; use the same unified Apply as other settings. |

### Select channels and sources

Use YAML version 1. A rule selects channels using exactly one of `numbers`,
`names`, `groups` or `profile`. Use numbers for individual channel numbers and
quoted strings for ranges, such as `numbers: [100, "102-105"]`. Names and group
names must match Dispatcharr entries. A named profile selects its enabled
members; `channels: {profile: all}` selects every eligible channel.

The outer `profile` (default `all`) limits which channels are eligible for rule
selection; it does not select or record those channels by itself. A nonblank
configuration with no rules selects no channels. Each rule's selector must
resolve to at least one eligible channel, and concrete rule selectors may not
overlap. `channels.profile: all` also sets the baseline source policy for all
eligible channels: use every source already assigned to each channel. A
specific rule replaces that baseline policy for its selected channels.

The following selector forms are examples. For the combined example, choose
values that resolve to disjoint channels in your catalog:

```yaml
version: 1
profile: "Synthetic Scope"  # Optional outer scope: enabled members only.
rules:
  - channels:
      numbers: [100, "102-105"]
  - channels:
      names: ["Synthetic Channel A"]
  - channels:
      groups: ["Synthetic Group"]
  - channels:
      profile: "Synthetic Enabled Profile"
```

To select all channels eligible under the outer profile and keep all of their
assigned sources eligible, add an explicit `channels.profile: all` rule:

```yaml
version: 1
profile: all
rules:
  - channels:
      profile: all
```

`include` and `exclude` refer to exact M3U source names already assigned to a
channel. They are mutually exclusive. `exclude: []` allows every assigned
source. `priority` is an integer score for sources permitted by that rule;
higher scores are tried first, missing scores are zero, and equal scores keep
Dispatcharr's channel stream order. A source filter never assigns a source to a
channel and never selects a source outside the allowed set. A filtered or
prioritized rule may use a dedicated recorder and require another provider
slot.

```yaml
version: 1
profile: all
timezone: Etc/UTC  # Optional IANA timezone for schedules.
schedule:
  weekdays:
    - start: "18:00"
      end: "23:00"
  weekend:
    - start: "10:00"
      end: "13:00"
rules:
  - channels:
      numbers: ["100-102"]
    include: ["Synthetic M3U A", "Synthetic M3U B"]
    priority:
      "Synthetic M3U A": 100
      "Synthetic M3U B": 50
    schedule:
      weekdays:
        - start: "18:00"
          end: "23:00"
      weekend:
        - start: "10:00"
          end: "13:00"
      friday:
        - start: "19:00"
          end: "23:30"
  - channels:
      groups: ["Synthetic Group"]
    exclude: ["Synthetic M3U Backup"]
    schedule: continuous
```

Replace the synthetic channel, group and source values with exact entries from
your Dispatcharr catalog. The outer schedule applies to the selected channels
unless a rule supplies its own schedule. The first rule's schedule replaces the
global one: channels 100–102 use its weekday and weekend windows, with the
explicit Friday window replacing that rule's weekday window. Channels in
`Synthetic Group` record continuously, excluding the named source.

### Schedule behavior

Top-level and per-rule schedules accept `daily`, `weekdays`, `weekend` and
explicit weekday keys (`monday` through `sunday`). For each day, an explicit
weekday entry overrides its group, which overrides `daily`. A day without a
matching entry is off. A rule schedule replaces the global schedule for that
rule; it does not merge with it. An omitted schedule means continuous recording
when no global schedule is present. `schedule: {}` means off; `schedule:
continuous` means always on.

Times are local wall-clock values in the selected IANA timezone. The timezone
is resolved when applying configuration in this order: valid YAML `timezone`,
valid container `TZ`, then a detectable timezone from the container's system
configuration. The resolved timezone is stored with the active schedule;
changing the container timezone later takes effect after another Apply.

Each window has `start` and `end` in `HH:MM` format. The end is exclusive;
`24:00` is permitted only as an end time. Equal start and end are invalid. An
end earlier than its start crosses midnight and belongs to its start weekday.
Adjacent or overlapping windows merge. During daylight-saving transitions,
nonexistent local times create no instants and repeated times apply in both
occurrences.

### Validate and apply

1. Edit the complete set of settings, including YAML and archive values.
2. Choose **Validate configuration** to preview selected channels, source
   policy and schedule. Validation errors include the field and, when available,
   line number.
3. Choose **Apply configuration** to activate the validated set together.
4. Choose **Archive status** to check recorder and archive state.

The YAML document is limited to 64 KiB and 256 rules. Duplicate or unknown keys,
unknown fields, invalid selectors, tags, anchors, aliases and YAML merge keys
are rejected. A failed validation keeps the active settings. A failed or
interrupted activation denies new recorder admission until recovery succeeds.

Changing the archive path does not move or delete old recordings or access
tokens. They remain at the previous path; copy archive data deliberately and
update your storage mounts before changing the setting.

## Recorders and controls

- **Pause recording** immediately pauses recorders and fences queued recording
  work. It does not stop archive playback or retention cleanup.
- **Resume recording** immediately resumes according to the active configuration
  and schedule.
- **Reconcile recorders** asks Catchuparr to compare desired recorders with the
  current workers and queue the needed changes.
- **Archive status** reports recorder state, indexed archive age and storage
  information. “Oldest indexed footage” is the age of the oldest indexed item;
  it does not promise uninterrupted recording for that entire span.

When a schedule closes, the recorder stops and releases its provider reservation.
Archive playback and retention continue. The archive storage limit applies to
Catchuparr's total archive data; older data is removed according to retention
and space pressure.

## Create M3U and XMLTV links

1. Set **User ID for new M3U token** to an existing Dispatcharr user.
2. Set **Public base URL for token links** to a URL playback clients can reach.
   If Dispatcharr is behind a reverse proxy or served under a path prefix, make
   sure proxy forwarding preserves the correct external scheme and prefix.
3. Apply the settings.
4. Choose **Create M3U/XMLTV access token**. Copy the separately displayed M3U
   playlist and XMLTV EPG links from the confirmation.
5. Add the M3U link and, if the player does not read its XMLTV URL automatically,
   the XMLTV link to the playback client.

Both links use the same token and the selected Dispatcharr user's permissions.
The user must have access to the relevant channel, M3U/EPG access, catch-up
permission and an available stream/session slot. A token does not grant extra
Dispatcharr permissions. The token is shown when created; treat the links like
passwords and do not put real tokens into logs, screenshots, issues or source
files. Token revocation is an administrative operation with no plugin UI
action; creating another token does not revoke an existing one. Custom clients
may send the token in the `X-Catchuparr-Token` header instead of a query
parameter.

The generated playlist keeps Dispatcharr's live URLs and metadata. Selected
channels get catch-up entries when indexed archive coverage is available. The
XMLTV output preserves provider guide data and overlays complete local historical
programme entries where available. Current and future guide entries remain
available. If guide data or an indexed programme is unavailable, that archive
programme cannot be played.

## Playback behavior

Catchuparr serves archive playback from the same host/path used for its local
archive service. The M3U path uses HLS with MPEG-TS segments; byte-range reads
are available for supported routes. Native XC playback uses MPEG-TS. Catchuparr
does not transcode streams or claim client-specific output-profile support.

Start-over and seek positions use programme boundaries and available indexed
segments. Start-over can begin at the start of a programme that is still in
progress; playback stops at that programme's observed EPG end, and only the
already captured part is available. Completed programmes use the same indexed
archive and retention checks. Segment boundaries and EPG timing mean seeks are
not frame-accurate. Incomplete archive coverage, missing EPG data, expired
content or missing segments can prevent playback. Live playback remains handled
by Dispatcharr.

Pause and Resume in a client control that playback session. The plugin's global
Pause/Resume actions control recording only.

### XC adapter

The native XC adapter can add Catchuparr archive entries to the XC playlist and
serve local TS archive playback through Dispatcharr's guarded XC integration.
Use it when a client consumes Dispatcharr's XC interface and has been validated
with that client's XC behavior. M3U/XMLTV is the primary documented path.
Although native server behavior has synthetic integration coverage for both
supported Dispatcharr versions, actual XC client behavior remains a separate
acceptance check.

## Native Timeshift Stats

Both Stats settings default to enabled and are applied with the rest of the
configuration:

- **Show archive playback in Stats** adds active Catchuparr archive viewers to
  native Timeshift Stats. The displayed ID is opaque, generated by Catchuparr,
  and is not a playback credential. Successful playback heartbeats refresh the
  row; it expires after 180 seconds without one. Values the playback request
  does not provide, such as position or pause state, remain unknown.
- **Hide recorders in Stats** removes only positively identified Catchuparr
  recorder clients from the returned live details. It does not change provider
  reservations, live streams or client limits. Ordinary viewers on a shared
  channel remain visible.

The native Stop action accepts a Catchuparr display ID after Dispatcharr's
native administrator authorization and revokes that local playback lease. It
does not revoke the user's access token or stop other viewers.

## Logging and privacy

Set **Catchuparr log level** to `DEBUG`, `INFO`, `WARNING` or `ERROR`. It affects
Catchuparr logs only. Events use fixed names and sanitized fields; access tokens,
authenticated URLs, credentials and EPG text are not included. Avoid sharing
raw configuration, exported databases, playback URLs, tokens or guide data when
requesting help.

## Troubleshooting

| Symptom | Check |
| --- | --- |
| No recorder starts | Confirm YAML is nonblank, **Validate** and **Apply configuration** succeeded, recording is enabled, the schedule is open and the channel selector matches. |
| A channel is missing | Check the outer profile, enabled profile membership, exact channel/group name, number/range and overlapping rules. |
| A source is not used | Check the exact M3U source name and that Dispatcharr already assigns it to the channel. Filtered rules may need provider capacity. |
| Token creation fails or links are wrong | Set the Dispatcharr user ID and reachable public base URL, apply both, and check reverse proxy scheme/path forwarding. |
| Playlist works but archive is absent | Check user catch-up permission, selected channel, retention, indexed footage, archive status, shared storage and archive path permissions. |
| Playback returns an authorization or limit error | Check the user's active status, channel ACL, M3U/EPG and stream permissions, concurrent stream/session limits and token URL. |
| Archive playback does not appear in Stats | Apply **Show archive playback in Stats**, start a supported archive session and allow its successful heartbeat to register. |
| Recorders remain paused | Check **Recording enabled**, apply the settings, use **Resume recording**, then check status and worker logs for recovery errors. |

## Build from source

The repository build creates the same ZIP layout used by the release workflow:

```sh
python3 scripts/build_plugin.py
```

For local development, the project uses Python 3.10+ syntax and Python 3.12 in
CI. The primary checks are:

```sh
python3 -m unittest discover -s tests -q
python3 -m compileall -q catchuparr scripts tests
ruff check .
python3 scripts/build_plugin.py
```

## Optional native output archive (0.5.0)

Version 0.5.0 adds local archive metadata to Dispatcharr's native M3U/XMLTV
exports when **Add Catchuparr catch-up to native output** is enabled and applied.
It preserves the native channel/profile output and provider catch-up fields.
Native archive playback serves stored MPEG-TS segments; a live output profile
does not transcode archive media. The setting defaults off. The earlier
implementation passed 433 tests and full pinned native AIO probes; the final
parity correction passed four focused native 0.32.0 cases. User acceptance is
complete; required release CI remains pending. Read the
[guide](docs/candidate-0.5.0.md) before enabling it.

CI also runs synthetic integration against pinned Dispatcharr AIO images for
0.31.0 and 0.32.0. See [development and deployment](docs/development.md) for an
isolated development environment, upgrade and rollback guidance.

## Release workflow

Maintainers can start a release by pushing a `v<version>` tag or by choosing
**Build and publish release** under GitHub Actions. The workflow reuses the
complete CI workflow, including unit/lint/package checks and both pinned native
integration jobs. Publishing only proceeds for a commit contained in `main`
and when the tag, plugin manifest and built ZIP all have the same version.

For a manual run, select `main`, enter the version without its `v` prefix, and
choose whether the release should be a pre-release. A missing version tag is
created at the exact tested commit; an existing tag is accepted only when it
points to that same commit. Tags for suffixed versions such as `0.5.0-rc.1` are
automatically marked as pre-releases. The workflow refuses to move or replace
an existing tag.

The GitHub release includes `catchuparr-<version>.zip`, `SHA256SUMS`, and the
complete changelog from 0.2.0 through the release version. A pre-release is
excluded from GitHub's latest-stable designation. The workflow validates the
ZIP layout and checksum before publication.
