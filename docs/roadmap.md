# Catchuparr roadmap

Baseline: released 0.1.4, M3U/XMLTV + HLS real-player acceptance completed with Dispatcharr 0.32.0. Production deployment is outside this roadmap. Each wishlist item gets its own release, ordered from hardest to easiest.

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
| 0.2.1 | YAML filter engine: channel selection, M3U include/exclude and optional priorities | High | In progress; supersedes unreleased 0.2.0 |
| 0.3.0 | Catch-up playback in Dispatcharr Stats | High to medium | Planned |
| 0.4.0 | Recording windows by channel and weekday | Medium | Planned |
| 0.5.0 | Hide archive recorders in Stats | Medium to low | Planned |

Channel selection formerly planned for 0.6.0 is part of 0.2.1. The incomplete
0.2.0 candidate will not be published as the next release.

## Settings contract

A single native Plugin Settings text field contains the YAML filter document.
Archive path, retention, the global storage limit and playback user remain
separate fields. Validate previews every planned recorder and its permitted
source order; Apply atomically activates the complete resolved configuration.
Saving a draft does not change active recordings. UUIDs remain internal archive
identities, never a separate user selection.

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
- Unit, package and both pinned native AIO checks gate the Dev candidate.
  Recording selection, source enforcement and real-player regression gate release.

See [the YAML candidate guide](candidate-0.2.1.md) for the exact syntax and
installation/rollback procedure.

## 0.3.0 — playback Stats

### Behavior

- Add **Show archive playback in Stats**, enabled by default.
- Display channel, user, client IP, HLS/XC, session start, requested archive time and last successful playback fetch. Do not expose bearer URLs or usable session credentials.
- Reloads and seeks do not create duplicate viewers. Successful playback fetches refresh a display heartbeat; remove the entry after 180 seconds without activity. This describes observed requests, not proof of a frame playing or a pause state.
- Use reserved plugin entry IDs and guarded detail/Stop handling. Stopping archive playback must not stop a live channel or recorder.
- Keep existing admission/session accounting as the sole limit source. Synthetic display entries must not add native connection-count entries.

### Acceptance

REST and WebSocket agree. Test HLS/XC, reloads, seeks, pause, replacement/grace sessions, disconnect, restart, stale heartbeat cleanup and isolated Stop actions. Verify no duplicate limit counting and existing native controls cannot target a live worker through an archive entry.

## 0.4.0 — recording schedules

### Behavior

- Modes: continuous (default) or weekly schedule. Default timezone: `Europe/Berlin`.
- Global default windows plus full per-channel weekly overrides. A channel override replaces the global schedule; unspecified override days do not record. Multiple windows per day are supported and overlaps merge.
- Windows crossing midnight belong to their start weekday. Local wall time governs DST: missing times disappear, repeated times apply in both occurrences.
- Both reconciliation and the running recorder check eligibility. Start by the next 30-second reconciliation; stop through the existing recorder supervisor. Do not leave a recorder running after its window closes.
- Preserve existing archive availability outside recording windows. Keep missing programme starts and gaps explicit; closing a scheduled recorder must not advertise future segments as available.

### Acceptance

Synthetic clock tests cover weekdays, midnight, multiple/overlapping windows, DST changes and reboot inside/outside windows. Dev verifies start, stop, restart and archived playback outside the active window, with real-player regression.

## 0.5.0 — recorder visibility

### Behavior

- Add **Hide recorders in Stats**, enabled by default.
- Identify only server-registered Catchuparr recorder clients. Never hide every loopback IP or rely solely on a forgeable User-Agent.
- Hide recorder-only rows; preserve real viewers on shared workers. Adjust visible client counts consistently, including lists capped by Dispatcharr.
- Cover known REST, detail and WebSocket producers. Provider occupancy, operational ownership and actual connection-limit accounting remain accurate.

### Acceptance

Test recorder-only workers, shared workers, genuine local clients, more than ten clients, removal/restart and toggle changes. Hiding the recorder must neither close it nor release provider capacity.

## Deferred validation

Native XC real-player acceptance remains separate from the completed 0.1.4 M3U/HLS milestone. Automatic continuation in other clients, exact displayed frame timing and extended retention/rollback playback checks are not yet established. Releases must describe actual evidence rather than infer player compatibility from server tests.
