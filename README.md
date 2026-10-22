# Catchuparr


<p align="center">
  <img src="docs/assets/catchuparr-icon-256.png" width="128" height="128" alt="Catchuparr logo">
</p>

Catchuparr adds a local rolling catch-up archive and start-over playback to
[Dispatcharr](https://github.com/Dispatcharr/Dispatcharr). It records selected
channels to a shared local archive and provides authenticated M3U/XMLTV links.
It also includes guarded native XC playback hooks and optional reuse of a
compatible live connection while recording.

Dispatcharr remains responsible for live streams, EPG data, users, channel
permissions and provider assignments. Catchuparr does not create source
assignments, cloud storage or a transcoding service.

## Version and compatibility

- The package version is **0.6.9 prerelease candidate**. Publication and
  required GitHub CI remain pending. See [candidate 0.6.9](docs/candidate-0.6.9.md)
  and the [validation record](docs/validation.md).
- The minimum supported Dispatcharr version is **0.32.0**. The compatibility
  guard accepts well-formed versions at or above that minimum. The
  native compatibility acceptance remains pinned to 0.32.0; newer versions
  have not been verified by this candidate. API-specific guards still fail
  closed when the expected Dispatcharr interfaces do not match.
- The primary playback path is authenticated M3U/XMLTV with local HLS playback
  using MPEG-TS segments. Native XC is guarded; acceptance with a particular
  playback client is not established here.
- Playback checks the active Dispatcharr user, channel access, catch-up
  permission and existing stream/session limits. Seeking follows indexed
  segment boundaries; it is not frame-accurate. Catchuparr does not transcode
  output into a requested profile.

## Install or update

1. Download `catchuparr-<version>.zip` from a GitHub release once one is
   published.
2. In Dispatcharr's Plugins page, import the ZIP and choose its update or
   overwrite option when replacing an existing installation.
3. Follow Dispatcharr's restart or reload prompt so web and worker processes
   load the same version.
4. Configure the plugin, then use **Validate configuration** and **Apply
   configuration**.

The ZIP contains a `catchuparr/` package with `plugin.json`, `plugin.py` and
`logo.png`. Dispatcharr documents `logo.png` beside the plugin entry point as
its optional plugin-card logo; the manifest has no separate icon field. Do not
upload an individual Python file. See [Dispatcharr's plugin guide](https://github.com/Dispatcharr/Dispatcharr/blob/main/Plugins.md).

When upgrading from a version before the 0.2.1 YAML configuration, Catchuparr
clears the obsolete channel selection and requires a fresh Validate and Apply.
This legacy reset preserves the archive, recordings, general storage settings
and playback credentials.

## Settings overview

Plugin settings are edited in Dispatcharr. **Apply configuration** validates
and activates the complete settings draft as one generation. Editing or saving
a draft alone does not change active recording policy. Pause and Resume are
immediate operational actions.

| Setting | What it controls |
| --- | --- |
| **Channel and source filter (YAML)** | Channel selection, assigned M3U source include/exclude and priority, schedule and timezone. Blank YAML selects no channels. |
| **Share existing live connections** | Optional global reuse of compatible existing viewer connections on all selected channels. Disabled by default. |
| **Live stability delay** | Delay before later switches to a new viewer connection. The first eligible attach is immediate. |
| **Return delay after last viewer leaves** | Grace before returning to an allowed source. |
| **Minimum interval between switches** | Cooldown applied to later source switches. |
| **Archive path** | Local writable storage shared by Dispatcharr web and worker processes. Default: `/data/catchuparr`. |
| **Retention (hours)** | Archive retention from 1 to 720 hours. |
| **Storage limit (GiB)** | Archive size limit from 1 to 10240 GiB. |
| **User ID for new M3U token** | Existing Dispatcharr user to bind to a newly created token. |
| **Public base URL for token links** | Absolute reachable HTTP(S) origin, optionally with a path prefix. No credentials, query or fragment. |
| **Recording enabled** | Global recording state activated by Apply. Pause and Resume act immediately. |
| **Catchuparr log level** | Catchuparr-only `DEBUG`, `INFO`, `WARNING` or `ERROR` logging. |
| **Show archive playback in Stats** | Adds active Catchuparr archive viewers to native Timeshift Stats; enabled by default. |
| **Hide recorders in Stats** | Hides only positively identified Catchuparr recorder clients in live details; enabled by default. |
| **Add Catchuparr catch-up to native output** | Optional native Dispatcharr M3U/XMLTV annotation and covered local guide history; disabled by default. |

## Select channels and assigned sources

The filter field accepts strict YAML version 1. Each rule selects channels with
exactly one of `numbers`, `names`, `groups` or `profile`. Use integers for
individual channel numbers and quoted strings for number ranges, for example
`numbers: [100, "102-105"]`. Names and group names must match Dispatcharr. A
named profile resolves to its enabled members.

The outer `profile` limits which channels can be selected; it does not select
channels by itself. Blank YAML and a document with no rules select nothing. To
select every eligible channel, add an explicit `channels: {profile: all}` rule.
Each selector must resolve to eligible channels. Overlapping direct channel
rules and overlapping named profile rules are rejected. Direct rules by number
or name can override a named-profile rule regardless of YAML order; groups do
not act as profile exceptions.

```yaml
version: 1
profile: all
rules:
  - channels:
      profile: all
    exclude: ["Synthetic Backup Source"]
```

`include` and `exclude` refer only to exact M3U source names already assigned
to a channel, and cannot appear together. `exclude: []` leaves all assigned
sources eligible. An omitted filter inherits from a matching profile rule,
then from a `profile: all` rule. An explicit filter replaces the inherited one.

A `priority` map ranks allowed source names: higher integer scores are tried
first; unlisted sources have score zero; ties retain Dispatcharr stream order.
An omitted map inherits. An explicit map replaces the whole inherited map, and
`priority: {}` clears inherited priorities. A source filter cannot add an
assignment or make a disallowed source eligible. Filtered or prioritized
recording may need a separate provider slot.

### Named-profile inheritance

A named `channels.profile` rule can provide the base filter, priority and
schedule for enabled members. A direct number or name rule can override those
values in either YAML order. The outer profile remains an eligibility scope.

```yaml
version: 1
profile: all
rules:
  - channels:
      profile: "Synthetic Profile All"
    exclude: ["Synthetic Source B"]
    priority:
      "Synthetic Source A": 20
    schedule:
      daily:
        - start: "08:00"
          end: "18:00"
  - channels:
      names: ["Synthetic Channel A"]
    include: ["Synthetic Source B"]
    priority:
      "Synthetic Source B": 50
    schedule_mode: override_days
    schedule:
      weekdays:
        - start: "18:00"
          end: "22:00"
      sunday: []
```

A rule schedule uses `schedule_mode: replace` by default and replaces the whole
week; unspecified days are off. `schedule_mode: override_days` changes only the
specified days after group expansion and inherits the rest. An explicitly empty
day is off. A rule with `schedule_mode` must contain a schedule. Validate
previews show the resolved filter, priority and schedule origins; Apply stores
those resolved values.

## Recording schedules

Omit the global or rule schedule for continuous recording. A schedule can use
`daily`, `weekdays`, `weekend` and explicit weekday names. More-specific day
entries take precedence: explicit weekday, then group, then `daily`. A rule's
schedule replaces the global week unless `override_days` is selected.

```yaml
version: 1
profile: all
timezone: Etc/UTC
schedule:
  weekdays:
    - start: "18:00"
      end: "23:00"
  weekend:
    - start: "10:00"
      end: "13:00"
rules:
  - channels:
      numbers: [100]
    schedule_mode: override_days
    schedule:
      friday:
        - start: "19:00"
          end: "23:30"
      sunday: []
```

Intervals use local wall-clock time, have exclusive end times, and may cross
midnight. `24:00` is valid only as an end time. An overnight interval belongs to
its start day. Overlapping or adjacent windows are merged. During a daylight
saving repeated hour, the matching local time can occur twice; a skipped time
in a DST gap does not occur. Timezone resolution is a valid YAML `timezone`,
then a valid container `TZ`, then the detectable system timezone. Apply stores
the resolved timezone.

The YAML parser rejects unknown and duplicate fields, tags, anchors, aliases
and merges. Input is limited to 64 KiB and 256 rules. Validation errors name the
field and line without echoing the full YAML document.

## Smart live connection sharing (0.6.1 candidate)

Sharing is disabled by default. It uses one global setting and applies only to
channels selected by the source filter. It can reuse a
healthy, compatible native live connection that already has an ordinary viewer,
even when that viewer's assigned source is outside a recorder's `include`
list. This reuses that viewer's stream; it does not open a new connection to an
excluded source. If no reusable viewer connection exists, recording uses only
sources allowed by the filter. If none is allowed, the recorder waits paused.

The feature checks the existing Dispatcharr owner, assignment, account,
profile, input compatibility, worker heartbeat and published media. It attaches
an owned reader to that worker's published MPEG-TS buffer; it does not
initialize or reserve another upstream stream. The initial recorder attach to
an already eligible viewer is immediate. Later switches use the configured
stability and minimum-interval delays; viewer churn does not reset stability
for the same native connection. The default delays are 5 seconds for live
stability, 30 seconds after the last ordinary viewer leaves, and 60 seconds
between later switches. Each delay accepts an integer from 0 through 3600.
Zero disables that delay. Apply sharing settings together with the filter and
other settings.

Upgrades migrate uniform settings from the prior per-channel configuration.
If those channel values conflict, Validate and Apply report the conflict and
leave the current configuration active. Choose the global values and enable
**Confirm global sharing settings migration** to resolve it. New active
snapshots store only the global sharing settings.

After a borrow, the recorder waits until both the last-viewer grace and the
remaining minimum switch interval have elapsed, then returns to an allowed
source. If none is available, it stays paused. Explicit Stop, invalidated
authority and recorder failure take the cleanup path without waiting for normal
viewer cooldowns. A switch ends the current FFmpeg attempt and starts the next
with a segment discontinuity. The recorder's channel fence remains in force.

**0.6.1 is still a candidate.** The 0.6.0 feature's pinned synthetic AIO
acceptance passed; the 0.6.1 global settings and migration changes have focused
unit coverage. Required GitHub CI and publication remain pending. Synthetic
tests do not establish real-player acceptance. See
[candidate 0.6.1](docs/candidate-0.6.1.md) for the remaining gates.

## Continuous catch-up playback (0.6.2 candidate)

Catch-up playback can continue across programme boundaries using the same
authenticated archive session. M3U/XMLTV HLS playback does not require later
EPG rows and can append newly indexed segments for up to 24 hours from the
requested start. XC playback follows contiguous indexed TS segments within the
Dispatcharr duration limit. XC stops at a real archive gap and waits no longer
than 125 seconds for a next segment after the current edge. That bound covers
the recorder's 120-second media-idle timeout plus five seconds for indexing
and finalization; a missing recorder lease closes XC follow mode promptly.
HLS uses the same 125-second edge window and recorder lease, with a 24-hour
session bound. This candidate has focused synthetic coverage only; CI and
real-player acceptance remain pending.

## Access tokens and playback

1. Set **User ID for new M3U token** to an existing Dispatcharr user.
2. Set **Public base URL for token links** to the URL playback clients can
   reach. Behind a reverse proxy, preserve the trusted external origin and any
   path prefix.
3. Apply settings, then run **Create M3U/XMLTV access token**.
4. Copy the M3U and XMLTV URLs from the action result. Treat both URLs and the
   token as bearer credentials; do not paste them in issue reports or logs.

Routes are `/catchuparr/m3u` and `/catchuparr/xmltv`. A token can be supplied as
`X-Catchuparr-Token` or in the `access_token`/`token` query parameter. The
associated Dispatcharr user must still be active. Playback additionally checks
that user's channel access, catch-up permission and session limits. A token
creation action makes a new token; there is no token-revoke button in the
current UI.

M3U output provides live channels and adds catch-up metadata where local
indexed segments and user permissions allow it. The plugin M3U's archived
playback uses local HLS MPEG-TS segments. XMLTV combines the provider guide
with complete, covered local programme history; current and future guide data
is retained. Playback of an archived programme requires matching EPG bounds.
M3U catch-up duration is expressed in seconds and capped at 24 hours. Seeking
uses segment boundaries, so it is not frame-accurate.

### Native Dispatcharr M3U/XMLTV output

**Add Catchuparr catch-up to native output** is off by default. When enabled and
applied, eligible native M3U channels keep their live URLs and metadata; local
catch-up metadata is added only for selected channels with indexed archive
segments and user catch-up permission. Native XMLTV retains current/future and
other-channel provider history while merging complete covered local programme
history for eligible selected channels. If Catchuparr cannot authorize or
produce the local response, it does not replace the native provider response.

### Native XC

Catchuparr includes guarded native XC hooks for archive programme selection and
segment delivery. XC request duration is interpreted in minutes. Provider
history for other channels is retained. Native server checks do not establish
acceptance by every XC client. Test program selection, start-over, seeking and
programme changes in the intended client before relying on this path.

## Stats and archive status

- **Show archive playback in Stats** displays active archive viewers in native
  Timeshift Stats. IDs are opaque and stable across seeks and session refreshes.
  The view uses successful playback requests and a 180-second heartbeat expiry;
  it does not prove that video is displayed or report an unobserved pause or
  position.
- An administrator can stop a displayed archive viewer. That action revokes
  its local playback lease; it does not stop a live channel or recorder.
- **Hide recorders in Stats** hides only positively identified Catchuparr
  recorder clients from live details. It does not change provider reservations
  or the live stream.
- **Archive status** reports selected-channel running counts, archive storage
  and the age of the oldest indexed footage. That age is not continuous
  coverage or recorded duration.

## Actions

| Action | Use |
| --- | --- |
| **Archive status** | Show recorder state, selected/running counts, oldest indexed footage age and storage use. |
| **Reconcile recorders** | Queue reconciliation against the current applied configuration. |
| **Create M3U/XMLTV access token** | Create a token for the configured existing user and return both links. |
| **Validate configuration** | Check the complete draft and preview resolved channels, sources, priorities and schedules without applying it. |
| **Apply configuration** | Validate and activate the full draft as one generation. |
| **Pause recording** | Immediately pause recorder activity and fence stale queued starts. |
| **Resume recording** | Re-enable recording according to the applied configuration and schedule. |

Current recordings shows only channels scheduled at the current time, with a
compact recorder and source status. Its message omits channel numbers and uses
the first word of the validated M3U profile name; full numbers and names remain
in the structured rows. Today's recording schedule lists channels with today's
windows. Both actions retain their structured channel and summary fields for
callers that need the full details.
The host may collapse message line breaks; visible rows use a bounded
nonbreaking-space gap. Copying the message may retain nonbreaking spaces, which
can be replaced with ordinary spaces in the destination text.

## Troubleshooting

- **No channels record:** confirm the YAML has a matching selector, validate and
  Apply it, check recording enabled and Pause state, then review schedule and
  source eligibility.
- **No source can be selected:** check exact assigned source names and channel
  assignments in Dispatcharr. Catchuparr will not create assignments.
- **Filtered source cannot start:** a dedicated worker may need another
  provider slot. Check Dispatcharr's account/profile capacity.
- **Sharing stays idle:** confirm it is enabled for a selected channel and that
  an ordinary viewer is using a healthy, compatible assigned stream. Sharing
  never starts a source outside the allowed set.
- **Token links fail:** check the applied public URL, user ID, active user,
  channel permission, catch-up permission, stream limits and reverse-proxy path.
- **History is missing:** verify local recording coverage, retention, selected
  channels and EPG programme bounds. Incomplete coverage is not advertised as
  a full historical programme.
- **Native XC does not play:** validate against the actual client's programme,
  duration and seeking behavior; native server tests alone are not player
  acceptance.

## Development checks

The repository uses synthetic media and strict native hook checks. The normal
local gates are:

```sh
python3 -m unittest discover -s tests -q
python3 -m compileall -q catchuparr scripts tests
ruff check .
python3 scripts/build_plugin.py
```

Release CI runs the pinned Dispatcharr 0.32.0 AIO integration job. The 0.6.0
candidate passed its full synthetic native AIO integration, including
ordinary-viewer retention, first-viewer borrowing, cooldown timing, Stop and
Apply revocation, restart recovery, exact client cleanup, source switching,
and retryable acknowledgements. Required GitHub CI and publication are pending.
The synthetic server checks do not establish real-player acceptance.

See the [changelog](CHANGELOG.md), [release roadmap](docs/roadmap.md),
[validation record](docs/validation.md) and
[development guide](docs/development.md).
