# Changelog

Changes are listed from version 0.2.0 onward. Versions 0.2.0 through 0.3.1
were development candidates and were not published as GitHub releases. They are
included here to explain the feature history leading to 0.4.0.

## [0.6.2] - Prerelease candidate

### Fixed

- Continue authenticated catch-up playback across programme changes without
  requiring later EPG entries. M3U/XMLTV HLS playlists follow newly indexed
  segments up to a 24-hour session bound; XC streams follow contiguous indexed
  segments within Dispatcharr's duration limit.
- Keep XC live-edge waiting finite. A real archive gap ends the raw TS stream,
  and a stopped recorder ends follow mode after its bounded idle wait.

### Notes

- Requires Dispatcharr 0.32.0; unverified versions are refused. The plugin icon
  remains included. Required GitHub CI and publication are pending.
- Real-player programme-transition acceptance has not been established.

## [0.6.1] - Prerelease candidate

### Changed

- Replace connection-sharing YAML and per-channel overrides with global GUI
  controls for enablement and the three sharing delays. Preserve uniform legacy
  values; require explicit confirmation when legacy channel settings differ.
- Attach a newly initialized recorder immediately to a compatible existing
  viewer connection. Later switches continue to respect configured delays.
- Reuse successful exact-tree CI artifacts for release publication instead of
  repeating full integration acceptance after merge and during release.

### Fixed

- Recover missing Redis sharing authority from validated committed history
  without restoring old viewer grants or recorder leases. Reject malformed
  history and preserve concurrent or newer authority.

### Notes

- Requires Dispatcharr 0.32.0; unverified versions are refused. The plugin icon
  remains included.
- Required GitHub CI and publication are pending for this candidate.

## [0.6.0] - Prerelease


### Added

- Add optional, disabled-by-default sharing of compatible live connections
  already used by ordinary viewers. A dedicated reader consumes the native
  worker's published MPEG-TS buffer without creating another upstream stream
  or provider reservation.
- Add per-channel sharing enablement and live-stability, last-viewer-return
  and minimum-switch cooldowns, persisted with unified configuration Apply.
- Add fenced episode and operation state, signed one-use borrow capabilities,
  exact reader cleanup, retryable recovery and a distinct FFmpeg attempt switch
  signal with segment discontinuities.
- Include the existing Dispatcharr plugin icon in the packaged ZIP as
  `catchuparr/logo.png`.

### Notes

- The full synthetic native acceptance passed on the pinned Dispatcharr 0.32.0
  AIO, and the prerelease was published after required GitHub CI passed.
  See the [0.6.0 guide](docs/candidate-0.6.0.md).

## [0.5.1] - Prerelease candidate, not published

### Added

- Inherit source filters, priority maps and schedules from enabled named
  channel profiles, with direct channel exceptions and explicit day overrides.
- Preserve resolved configuration origins in validation previews and applied
  snapshots.

### Notes

- This candidate is not published. The 453-test suite and full pinned AIO probes
  passed for both supported Dispatcharr versions. Required CI and isolated Dev
  verification remain pending. See the [0.5.1 candidate
  guide](docs/candidate-0.5.1.md).

## [0.5.0] - 2026-10-12

### Added

- Add a disabled-by-default unified-Apply setting to annotate eligible native
  Dispatcharr M3U output with Catchuparr archive catch-up metadata and merge
  covered local programme history into native XMLTV output.
- Add signed channel/profile-scoped capabilities and native archive/segment
  routes that recheck current user access, network scope and session limits.

### Notes

- The 433-test suite and full native AIO probes passed before the final parity
  correction. That correction passed four focused native 0.32.0 cases. User
  acceptance is complete; required release CI remains pending and the version
  is not published.
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
