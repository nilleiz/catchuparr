# Catchuparr roadmap

Baseline: released 0.1.4, M3U/XMLTV + HLS real-player acceptance completed with Dispatcharr 0.32.0. Production deployment is outside this roadmap. Related features are grouped into releases; native output integration comes last.

## Workflow and release gates

- Use separate worktrees, small `develop/` feature branches and PRs. Merge after independent review and green required checks; fix failures without weakening checks.
- Every release includes an installable plugin ZIP, SHA-256 checksum, release notes and installation/update/rollback guidance.
- Relevant synthetic tests, lint, compile and package checks, plus integration probes in pinned Dispatcharr 0.31.0 and 0.32.0 AIOs, gate publication. New private hooks are version/signature checked, idempotent and removable.
- Deploy only to the isolated single-container Dev AIO, with its separate database, Redis, volumes, ports and restricted egress. Preserve consistent private backups and existing archives.
- Recording changes require real-player regression for start-over, seeking, pause/resume and programme transition. CI does not replace real-player acceptance.
- No companion container is planned; no production update or upstream PR is authorized by this roadmap.

## Release sequence

| Release | Feature | Relative effort | Status |
| --- | --- | --- | --- |
| 0.2.1 | YAML filter engine: channel selection, M3U include/exclude and optional priorities | High | Installed in isolated Dev; release acceptance remains pending |
| 0.3.0 | YAML schedules, global recorder control and consistent logging | Medium | Implemented; superseded by the 0.3.1 candidate and not published separately |
| 0.3.1 | Weekly schedule day groups, timezone resolution, unified Apply and M3U token links | Medium | Implemented candidate; independent review, CI and Dev/player acceptance remain pending |
| 0.4.0 | Playback Stats and independent recorder visibility control | High to medium | Implemented candidate; independent review, pinned native checks and player acceptance remain pending |
| 0.5.0 | Optional native Dispatcharr M3U/XMLTV archive integration | High | Planned last |

Channel selection formerly planned for 0.6.0 is part of 0.2.1. The incomplete
0.2.0 candidate is superseded and will not be published.

The 0.2.1 filter engine is included in the 0.3.1 candidate. Version 0.3.0 was
not published separately.

## Settings contract

A single native Plugin Settings text field contains the YAML filter document.
Archive path, retention, storage limit, playback user, public token-link base
URL, recording-enabled state and log level remain separate fields in the same
settings form. Validate previews the complete draft; one Apply validates and
activates all its fields
as one configuration generation. Saving a draft does not change active
recordings. Pause and Resume remain immediate operational actions. UUIDs remain
internal archive identities, never a separate user selection.

## 0.2.1 — YAML filter engine

- Select channels directly by number/range, exact name, group or Channel Profile.
  Named profiles include enabled members only; the outer profile defaults to all.
- Use profile: all as the explicit all-channel selector. Wildcards are rejected.
  The all-profile rule is a default; specific rules replace its whole policy.
  Other overlapping rules fail validation.
- Include/exclude are mutually exclusive; priorities are optional with either.
  Filter first, then sort by descending priority, retaining channel stream order
  for ties and when no priorities are supplied.
- No effective source restriction or priority preserves the shared live route.
  Overrides retain dedicated native workers, capacity limits and fenced cleanup.
- Start with empty configuration and require Apply before any recorder starts.
  No previous configuration or active snapshot is migrated or activated.
  Detected pre-0.2.1 filter settings and active snapshots are deleted on startup.
  General archive/storage settings, archive data and stable identities remain.
- Apply stores a resolved JSON snapshot; later numbering/profile membership
  changes require another Apply. Old queued tasks recheck current configuration.
- Strict YAML parsing rejects duplicate/unknown keys, invalid types, custom tags,
  anchors, aliases and merges, with a 64 KiB input limit and useful field errors.
- The Dev candidate is installed. Recording selection, source enforcement and
  real-player regression remain release-acceptance gates.

See [the 0.3.1 candidate guide](candidate-0.3.1.md) for the current syntax,
schedule, control, installation and rollback notes.

## Intermediate step — repository content cleanup (completed)

Tracked repository content was cleaned up and the approved historical rewrite
was completed. GitHub may retain pull-request refs or cached objects under its
own hosting rules; these are outside the repository's controls. Technical
diagnostics and reproducible validation remain preserved. No plugin release
was required for this content step.

## 0.4.0 — playback Stats

### Behavior

- Add **Show archive playback in Stats**, enabled by default.
- Display channel, user, client IP, HLS/XC, session start, requested archive time and last successful playback fetch. Do not expose bearer URLs or usable session credentials.
- Reloads and seeks do not create duplicate viewers. Successful playback fetches refresh a display heartbeat; remove the entry after 180 seconds without activity. This describes observed requests, not proof of a frame playing or a pause state.
- Use reserved plugin entry IDs and guarded detail/Stop handling. Stopping archive playback must not stop a live channel or recorder.
- Keep existing admission/session accounting as the sole limit source. Synthetic display entries must not add native connection-count entries.

### Acceptance

REST and WebSocket agree. Test HLS/XC, reloads, seeks, pause, replacement/grace sessions, disconnect, restart, stale heartbeat cleanup and isolated Stop actions. Verify no duplicate limit counting and existing native controls cannot target a live worker through an archive entry.

## 0.3.0 — recording schedules, recorder control and logging

### Behavior

- Modes: continuous (default) or weekly schedule. Default timezone: `Europe/Berlin`.
- Store schedules in the existing YAML configuration: global default windows plus full per-rule weekly overrides. A rule override replaces the global schedule; unspecified override days do not record. Multiple windows per day are supported and overlaps merge.
- Windows crossing midnight belong to their start weekday. Local wall time governs DST: missing times disappear, repeated times apply in both occurrences.
- Both reconciliation and the running recorder check eligibility. Start by the next 30-second reconciliation; stop through the existing recorder supervisor. Do not leave a recorder running after its window closes.
- Preserve existing archive availability outside recording windows. Keep missing programme starts and gaps explicit; closing a scheduled recorder must not advertise future segments as available.

- Add a durable global recording switch and explicit pause/resume actions, independent of plugin activation. Keep archive playback, exports and retention active while paused; resume only applied selections within eligible windows.
- Validate previews schedules; Apply activates selection, source policies and schedules atomically. A missing schedule records continuously; an explicitly empty weekly schedule records nothing.
- Add consistent `[Catchuparr]` logging using Dispatcharr formatting, configurable level (default INFO), sanitized events and bounded repeated failures.

### Existing 0.3.0 schedule example

The following syntax is available in the 0.3.0 candidate. It demonstrates multiple
windows in both the global schedule and a per-rule override. It does not use the
planned 0.3.1 day-group or timezone-default behavior.

```yaml
version: 1
timezone: Europe/Berlin
schedule:
  monday:
    - start: "06:00"
      end: "08:00"
    - start: "08:00"
      end: "09:00"
    - start: "20:00"
      end: "22:00"
  sunday:
    - start: "23:30"
      end: "01:00"
rules:
  - channels:
      numbers: [100]
    include: ["Synthetic M3U A"]
    schedule:
      monday:
        - start: "07:00"
          end: "08:30"
        - start: "08:00"
          end: "09:30"
        - start: "20:00"
          end: "21:30"
      sunday:
        - start: "00:00"
          end: "02:00"
        - start: "23:30"
          end: "01:00"
```

Adjacent or overlapping windows merge within each schedule: the global Monday
windows from 06:00 through 09:00 form one interval, and the rule's Monday
windows from 07:00 through 09:30 form one interval, alongside their later
windows. For channel 100, the rule schedule replaces the global schedule as a
whole; its unlisted days remain off. Other selected channels continue to use the
global schedule. The Sunday overnight interval belongs to Sunday, its start day.

### Acceptance

Synthetic schedule/control tests and full native integration on both pinned AIO
versions passed for the current candidate. Independent review, CI and Dev/player
acceptance remain pending before release. Real-player regression must verify
start, stop, restart and archived playback outside the active window.

## 0.3.1 — schedule shorthand, timezone resolution and unified Apply

This is the current implementation candidate. It preserves the existing
continuous-recording default for a missing schedule and adds the following
behavior. Version 0.3.0 was not published separately.

### Candidate behavior

- Accept `daily` for Monday through Sunday, `weekdays` for Monday through Friday,
  and `weekend` for Saturday and Sunday. Each key takes the same window list as
  an explicit day and applies that list to every day in its group.
- Expand the group keys into the existing per-day schedule before applying the
  timezone, overnight-window and DST rules.
- When entries overlap, use this replacement order for each day:
  `daily`, then its matching group (`weekdays` or `weekend`), then that day's
  explicit entry. A more-specific entry replaces the entire window list for
  that day; other days in the group keep their group windows.
- In an explicit weekly schedule, a day with no entry after expansion remains
  off. A missing schedule keeps the existing 0.3.0 continuous behavior.
- Keep existing explicit weekday schedules valid without changes.
- Resolve the schedule timezone in this order: explicit YAML timezone,
  valid container `TZ`, then the detectable system timezone. A valid YAML value
  takes precedence; an invalid explicit YAML value fails with a clear
  validation error. If YAML is omitted and a supplied `TZ` is invalid, fail
  clearly; if no `TZ` is supplied, require a detectable valid system timezone.
  Do not assume the host timezone or fall back to a fixed `Europe/Berlin` value.
- The development Compose file accepts an optional `TZ` pass-through and does
  not set a fixed timezone by default.
- Provide one unified configuration Apply for the complete settings draft:
  filter YAML and schedules, timezone, global recording-enabled value, log
  level, archive path, retention, storage limit, playback user and optional
  public base URL for token links. Do not add
  separate settings Apply actions for individual fields or groups. Pause and
  Resume may remain immediate operational actions, not a second settings Apply.
- Validate the full draft before activation. A validation or persistence error
  must not partially activate fields or a new configuration generation. The
  database draft and filesystem state are separate stores; activation uses the
  shared lock, staging/rollback and fail-closed admission marker, and does not
  claim a single database/filesystem transaction.
- After creating an M3U access token, show two clearly labeled links in the
  confirmation toast: the authenticated M3U playlist URL and XMLTV EPG URL. Both
  links use the same newly created token and its existing user permissions. Put
  them on separate lines and make them selectable and copyable; add copy controls
  when the toast UI supports them. Never write tokens or authenticated URLs to
  logs or repository files.

### 0.3.1 examples and workflow

The following day-group syntax is part of the 0.3.1 candidate. A more-specific
day replaces that day's full group list.

```yaml
version: 1
timezone: Etc/UTC  # Explicit YAML timezone takes precedence.
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
```

Here `daily` supplies the baseline, `weekdays` replaces it Monday through
Friday, and `weekend` replaces it Saturday and Sunday. The explicit Monday list
then replaces Monday's weekday list. Other weekdays keep the `weekdays` list;
Saturday and Sunday keep the `weekend` list. Overnight windows still belong to
their start day, and normalized overlaps or adjacent intervals merge. An
explicit YAML timezone wins over container `TZ`; when the YAML field is
omitted, a valid container `TZ` is used, then the detectable system timezone.
Invalid or unavailable timezone resolution fails clearly. Previously applied
legacy snapshots keep their stored timezone until a successful new Apply.

For the 0.3.1 unified settings workflow, set the optional public base URL if
creating token links, then edit one complete draft, validate
the resolved preview, then choose **Apply configuration** once. That Apply
activates YAML filters, schedules, timezone, recording-enabled, logging and the
other settings together; saving alone does not activate the draft, and a failed
validation or persistence operation must not leave a partial configuration.
Pause and Resume remain immediate operational actions. Token creation remains
a separate action; its confirmation presents both links, for
example:

```text
M3U playlist URL: [copyable authenticated link]
XMLTV EPG URL: [copyable authenticated link]
```

Both links use the same created token and its existing user permissions. Never
log the token or either authenticated URL.

### Acceptance

The implementation tests `daily` combined with `weekdays` and `weekend`,
explicit-day replacement of group windows, unaffected days in the same group,
days with no
matching entry, window overlap normalization, environment timezone selection,
system timezone detection, explicit YAML precedence, invalid/undetectable
timezone errors, and DST gaps and repeated local times under the resolved zone.
Verify that one Apply validates and activates all settings together; invalid
YAML and simulated multi-setting persistence failures leave the prior active
configuration intact. Test pause/resume control-generation fencing against a
concurrent Apply, shared configuration-generation checks, and rejection of
stale queued recorder tasks. Documentation distinguishes historical 0.3.0
behavior from the 0.3.1 candidate, and shows global and per-rule multiple
windows, overnight ownership, interval merging and whole-schedule override. It
must also show `daily`, `weekdays`, `weekend` and an explicit-day override, the
explicit-YAML versus environment/system timezone resolution, and the single-Apply
workflow. Verify token creation displays both the M3U playlist and XMLTV EPG
links with clear labels, both links map to the same token and existing
permissions, and the toast leaves each link selectable and copyable without
truncation. Check that application logs contain neither the token nor either
authenticated URL.

## 0.4.0 — recorder visibility

### Behavior

- Add **Hide recorders in Stats**, enabled by default and independent of **Show archive playback in Stats**. Test all four combinations.
- Identify only server-registered Catchuparr recorder clients. Never hide every loopback IP or rely solely on a forgeable User-Agent.
- Hide recorder-only rows; preserve real viewers on shared workers. Adjust visible client counts consistently, including lists capped by Dispatcharr.
- Cover known REST, detail and WebSocket producers. Provider occupancy, operational ownership and actual connection-limit accounting remain accurate.

### Acceptance

Test recorder-only workers, shared workers, genuine local clients, more than ten clients, removal/restart and toggle changes. Hiding the recorder must neither close it nor release provider capacity.

## 0.5.0 — native output integration

- One settings toggle, disabled by default, enables local catch-up attributes and archived EPG in native Dispatcharr M3U/XMLTV links.
- Keep the full existing channel list, profiles, permissions, live URLs and provider catch-up. Separate plugin links remain available independently.
- Disable the toggle to restore original output without local additions. Toggle changes require no container restart.
- Use version-checked, idempotent and removable wrappers; test authorization, gaps, duplicate EPG entries and both toggle states.

## Deferred validation

Native XC real-player acceptance remains separate from the completed 0.1.4 M3U/HLS milestone. Automatic continuation in other clients, exact displayed frame timing and extended retention/rollback playback checks are not yet established. Releases must describe actual evidence rather than infer player compatibility from server tests.
