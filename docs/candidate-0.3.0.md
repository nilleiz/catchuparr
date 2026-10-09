# 0.3.0 recording-schedule candidate

This is an unreleased candidate. The manifest and generated ZIP use version
`0.3.0`. Synthetic native integration passed on Dispatcharr 0.31.0 and 0.32.0.
Required CI and real-player acceptance remain open; do not treat the candidate
as a published release.

## YAML schedules

Use the existing **Channel and source filter (YAML)** setting. Schedules share
the same document and Apply operation as channel selection and source filters.
The global timezone defaults to `Europe/Berlin`. If no global `schedule` is
provided, recording is continuous. A top-level empty map means no recording.
Each rule may provide its own schedule; that schedule replaces the global one
for the rule's channels. Without a rule schedule, the global schedule applies.

```yaml
version: 1
timezone: Europe/Berlin
schedule:
  monday:
    - start: "20:00"
      end: "22:00"
  sunday:
    - start: "23:30"
      end: "01:00"
rules:
  - channels:
      numbers: [100]
    include: ["Synthetic M3U A", "Synthetic M3U B"]
    priority:
      "Synthetic M3U A": 100
      "Synthetic M3U B": 50
  - channels:
      numbers: [101]
    include: ["Synthetic M3U B"]
    schedule: {}
```

The example uses invented channel and account values. Replace its selectors and
M3U names with exact entries from the current Dispatcharr catalog. Include and
exclude filters apply only to M3U sources already assigned to a channel;
Catchuparr does not create source assignments. A filtered or prioritized source
policy uses a dedicated worker and may require another provider slot.

Windows are local wall-clock times in the selected IANA timezone. End times are
exclusive. An end earlier than its start is an overnight interval belonging to
the start weekday. `24:00` is allowed only as an end; equal start and end times
are invalid. Adjacent and overlapping windows merge. A rule's explicit
`schedule: {}` disables recording for its selected channels; it does not erase
the global schedule for other rules. Use `schedule: continuous` to override a
weekly schedule for one rule.

Use **Validate** to preview the resolved channels, source policy and schedule.
**Apply configuration** atomically stores the resolved selection and schedules.
Saving a draft does not activate it. A successful Apply is required after
editing YAML or after a legacy-configuration reset. Blank YAML is valid and
selects no channels. Existing version-2 active snapshots remain valid and are
read as continuous schedules until the next successful Apply writes a version-3
snapshot.

On startup, the plugin removes pre-0.2.1 channel/source filter settings and
version-1 active snapshots without converting them. It preserves archive data,
recordings, playback credentials and general archive settings. After this
one-time reset, no recorder starts until YAML is validated and applied.

## Global recording control

The **Recording enabled** setting defaults to true. Saving a changed value alone
does not change running recorders. Use **Apply recorder control** to apply the
currently saved setting, independently of whether the YAML draft validates.
The **Pause recording** and **Resume recording** actions apply immediately and
update the setting. These actions fence queued work and stop active recording
when paused. Old queued jobs are rejected before they allocate recording
resources.

Pausing recording does not disable archive playback or retention. A closed
weekly schedule also stops the recorder and releases its provider reservation;
playback and archive retention continue under their existing settings. A
schedule change takes effect through **Apply configuration**.

## Logging

The **Catchuparr log level** setting defaults to `INFO` and accepts `DEBUG`,
`INFO`, `WARNING` or `ERROR`. It controls only the Catchuparr logger hierarchy.
Recorder-control and schedule events use the `[Catchuparr]` prefix with fixed
event names and allowlisted state fields. Repeated errors are rate-limited.
These events do not include playback URLs, credentials, tokens or EPG text.

## Build and acceptance

Build with `python3 scripts/build_plugin.py`; the resulting filename follows the
plugin manifest version. Before importing the candidate, check the generated ZIP
contents and checksum. Follow the repository's isolated-install and rollback
procedure in [development and deployment](development.md).

That procedure backs up application/configuration data only by default, excludes
the archive directory, recordings and archive database, and keeps at most two
verified task-owned Dev restore points. Archive inclusion requires explicit
authorization. Rollback restores the previous package/image and its matching
application/configuration restore point while preserving the current archive;
that restore point cannot recover archive content that was lost or overwritten.

The candidate's synthetic native probes passed in both pinned Dispatcharr
versions for schedule closure, global pause/resume while running, stale-job
fencing, failover media, concurrent live/archive isolation and cleanup. The
full local suite, lint, compile and package checks also passed. Green CI and
real-player acceptance remain release gates; no 0.3.0 release or player
compatibility claim is made here.
