# Changelog

Changes are listed from version 0.2.0 onward. Versions 0.2.0 through 0.3.1
were development candidates and were not published as GitHub releases. They are
included here to explain the feature history leading to 0.4.0.

## [0.5.0] - Development candidate, not published

### Added

- Add a disabled-by-default unified-Apply setting to annotate eligible native
  Dispatcharr M3U output with Catchuparr archive catch-up metadata and merge
  covered local programme history into native XMLTV output.
- Add signed channel/profile-scoped capabilities and native archive/segment
  routes that recheck current user access, network scope and session limits.

### Notes

- The 433-test local suite and full native AIO probes on pinned Dispatcharr
  0.31.0 and 0.32.0 pass. Required CI, isolated Dev verification and
  player-specific acceptance remain pending; this candidate is not published.
  See the [0.5.0 candidate guide](docs/candidate-0.5.0.md) for boundaries and
  status.

## [0.4.0] - 2026-10-12

### Added

- Show active Catchuparr archive viewers in native Dispatcharr Timeshift Stats.
- Hide positively identified Catchuparr recorder clients from live Stats
  details, with a separate setting from archive-viewer display.
- Let a native administrator stop a displayed Catchuparr archive viewer. The
  operation revokes that local playback lease.
- Keep stable opaque display IDs for viewers across seeks and session
  replacement; expire inactive Stats rows after 180 seconds without a
  successful playback heartbeat.
- Document all plugin settings, actions, configuration examples, scheduling,
  playback, security and troubleshooting in the README.
- Build and publish a versioned plugin ZIP and SHA-256 checksum through the
  release workflow after all existing CI and native integration jobs pass.

### Notes

- Both new Stats settings default to enabled and use unified configuration
  Apply. The hooks are signature checked against Dispatcharr 0.31.0 and 0.32.0.
- Position, pause state and stream details remain unknown when playback does not
  provide an observed value. Recorder hiding changes the returned Stats copy;
  it does not change provider reservations or live stream state.
- Native XC player acceptance remains a separate validation step.

## [0.3.1] - Development candidate, not published

### Added

- Add `daily`, `weekdays` and `weekend` schedule groups alongside explicit
  weekday entries. More-specific day entries replace the group or daily list.
- Resolve schedule timezone from explicit YAML, a valid container `TZ`, or a
  detectable system timezone, then store the resolved timezone on Apply.
- Add a unified configuration Apply for YAML, schedules, timezone, recording
  enabled, log level, archive path, retention, storage limit, playback user and
  public token-link URL.
- Add immediate Pause and Resume recording actions with stale queued-work
  fencing.
- Add public-base-URL configuration and clearly separated M3U/XMLTV token links.

### Changed

- Reject duplicate or unknown YAML fields, tags, anchors, aliases and merges;
  report field and line information without echoing configuration contents.
- Keep the previously applied configuration active after a validation failure;
  deny recorder admission during incomplete activation recovery.

## [0.3.0] - Development candidate, not published

### Added

- Add weekly schedules globally and per channel rule, with explicit weekday
  windows, overnight intervals, `24:00` end times and a continuous mode.
- Stop scheduled recorders outside their windows and release their provider
  reservation while keeping playback and retention available.
- Add a global recording-enabled control and an immediate Pause/Resume control.
- Add configurable Catchuparr log levels and sanitized recorder-control events.

### Changed

- Apply schedule changes with the channel/source YAML configuration.
- Define schedule windows as local wall-clock time with exclusive ends and
  merging of overlapping or adjacent intervals.

## [0.2.1] - Development candidate, not published

### Added

- Replace the earlier separate channel selection with a versioned YAML
  configuration for channel numbers, names, groups and enabled channel
  profiles.
- Add per-channel-rule M3U source include and exclude filters and source
  priorities.
- Add a Validate preview and an Apply operation for resolved channel and source
  selections.
- Use dedicated recorder workers when an effective source filter or priority
  requires control over the selected source; preserve the allowlist during
  fallback.

### Changed

- Require exact source names already assigned in Dispatcharr; do not
  automatically create or map source assignments.
- Reset obsolete pre-0.2.1 filter selections on first upgrade and require a
  fresh validated Apply. Existing archive data, recordings, general storage
  settings and playback credentials are preserved.

## [0.2.0] - Superseded candidate, not published

- Prototyped separate channel UUID selection and source rules. This interface
  was superseded by the unified YAML configuration in 0.2.1 and is not part of
  the current configuration format.

## Earlier

- The 0.1.4 release introduced the M3U/XMLTV archive playback milestone with
  HLS archive playback. See the
  [0.1.4 release](https://github.com/nilleiz/catchuparr/releases/tag/v0.1.4).
