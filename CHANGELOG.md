# Changelog

Changes are listed from version 0.2.0 onward. Versions 0.2.0 through 0.3.1
were development candidates and were not published as GitHub releases. They are
included here to explain the feature history leading to 0.4.0.

## [0.7.1] - Prerelease candidate

### Fixed

- Keep healthy borrowed connections attached when the native buffer advances
  between Redis reads. Observe the published chunk before its index bound;
  retain all owner, lease, source, client and missing-chunk rejection checks.
  Emit fixed observation rejection categories instead of misreporting every
  failure as an owner/source/profile change.
- Map independent MPEG-TS PES/PCR clocks onto one archive-relative XC timeline.
  Preserve byte offsets and HTTP Range semantics while allowing playable media
  after gaps and source/muxer changes. Missing archive time remains a forward
  clock jump; it is never filled with live or repeated media.
- Keep native catch-up playlists appendable while their bounded programme is
  still recording. Close completed historical windows and exclude later shows.
- Mark fresh recorder muxer attempts as discontinuities even when their archive
  wall-clock intervals are adjacent.
- Retain authentication, lease/session cancellation and raw-mode gap rejection.
  Mixed or unmappable clocks across unsafe boundaries fail closed.

### Verification

- Synthetic audio/video regression checks decoder output, programme duration,
  timestamp wraparound, and byte ranges across independently muxed segments.
- Native 0.32.0/0.33.0 CI and external-player acceptance remain pending for this
  candidate. No production deployment is implied by source preparation.
- Missing recordings are not reconstructed. The buffer-progress invalidation
  is reproducible with the old source; individual historical failures cannot
  all be uniquely attributed because older logs omit the rejected predicate.

## [0.7.0] - Prerelease candidate

### Fixed

- Export native catch-up with an HLS suffix and optional duration. Resolve known
  unexpanded duration placeholders using guide bounds or a bounded snapshot;
  keep client `lutc` separate from programme end.
- Close historical native playlists at the requested end instead of exposing
  later live media. Explicit `playback=continuous` retains continuous playback.
- Admit local XC media at the requested start even when later archive time is
  missing. Serve only the contiguous prefix, mark partial responses with
  `X-Catchuparr-Archive-Partial: true`, and stop before gaps or discontinuities.
  Numeric XC durations remain minutes; native durations remain seconds.
- Preserve authentication, current channel/network policies, session limits,
  and provider fallback when no local starting media is available.

### Verification

- Focused synthetic regressions cover placeholder handling, historical media
  isolation, finite XC windows, partial archives and missing starts. Native CI
  checks these paths on Dispatcharr 0.32.0 and 0.33.0 before publication.
- HLS recognition depends on client support. Real-player acceptance is not
  implied by synthetic or native integration results. Missing archive material
  is not recovered by this change and partial responses may end early.

## [0.6.10] - Prerelease candidate

### Fixed

- Use the shared channel-access policy introduced in Dispatcharr 0.33.0 for
  local XC playback. Preserve the legacy 0.32.0 helper when the shared module
  is absent. A shared-policy denial or failure never falls back to an older,
  less restrictive visibility check.
- Keep the local XC playlist timestamp parameter as `utc` on newer supported
  cores, instead of selecting `start` for every version except exactly 0.32.0.

### Verification

- Extend authoritative native CI to pinned Dispatcharr 0.32.0 and 0.33.0
  images. Require both executed native steps for trusted release-artifact
  reuse, alongside the successful package job.
- Preserve the 0.32.0 minimum-version policy and API capability checks. Native
  probes use the supported UTC query contract and distinguish the core's
  legacy authentication denials from its newer unauthorized/not-found rules.
- Required native acceptance is pending for this candidate.

## [0.6.9] - Prerelease candidate

### Fixed

- Recover a superseded active borrow episode before admitting another recorder
  opening after restart. Exact current lease and applied authority, an older
  episode fence, absent native projection and confirmed private-client absence
  are required. A permanent native tombstone prevents delayed registration from
  restoring the obsolete episode before SQLite acknowledges its cleanup.
- Preserve ordinary viewers and native source ownership. Missing cleanup proof,
  stale admission, SQL contention or Redis failure keeps new openings blocked;
  existing intent claims are never deleted or reused.

### Verification

- Focused real-SQLite and Redis/Lua regressions cover obsolete active state,
  fresh registration, preserved viewers, lease and authority changes, corrupt
  state, cleanup retries and opening rejection before recovery.
- Requires Dispatcharr 0.32.0 or newer. Authoritative native CI is pending.

## [0.6.8] - Prerelease

### Fixed

- Retain the recorder task, lease and allowed-source position while retrying
  delayed revocation, native stop and HTTP cleanup acknowledgments. A failed
  first source no longer forces reconciliation to restart at that source
  instead of advancing to the next permitted source after confirmed cleanup.
- Continue checking pause, configuration and lease authority during cleanup
  without renewing an intentionally revoked recorder capability. Unconfirmed
  cleanup never authorizes another source.

### Verification

- Synthetic regressions cover delayed native stop and HTTP acknowledgment,
  transient cleanup errors, cancellation and lease loss. The native
  Dispatcharr 0.32.0 scenario withholds real cleanup acknowledgments, checks
  the unchanged lease and absence of replacement connections, then requires
  useful indexed media from the next permitted source.
- Requires Dispatcharr 0.32.0 or newer.

## [0.6.7] - Prerelease candidate

### Fixed

- Restore missing Redis sharing-authority projections during recorder
  reconciliation from exact, committed outbox heads after a Redis/container
  restart. Recovery fails closed on pending transitions, mismatched generations
  or newer conflicting projections, and never replays prior episode snapshots.
- Recover from a due RETURN intent after its exact borrowed episode is closed
  when a different, currently eligible viewer is available. Recovery rechecks
  the current schedule, lease, configuration, control generation, source
  catalog and cleanup proof, then creates a fresh single-use ATTACH intent.
- Preserve the existing live-stability and minimum-switch cooldowns; the old
  RETURN claim is never reused, and an unchanged viewer or unconfirmed cleanup
  does not supersede the pending return.

### Notes

- Requires Dispatcharr 0.32.0 or newer. Required GitHub CI and native
  acceptance remain pending.

## [0.6.6] - Prerelease candidate

### Fixed

- Resume the same admitted ATTACH intent after a transient pre-claim validation
  miss when its durable claim remains unused. The current viewer and recorder
  authority are revalidated, and the SQLite claim remains single-use.

### Notes

- Requires Dispatcharr 0.32.0 or newer. Publication and native acceptance are
  pending.

## [0.6.5] - Prerelease candidate

### Fixed

- Preserve the persisted RETURN admission deadline through first-media
  completion, including the failure-cleanup cooldown bypass. The media
  timestamp remains fixed, and normal returns still observe their admitted
  viewer-return and minimum-switch deadlines.
- Retain the safe exception class and call site when a RETURN completion save
  fails without an exact committed transition.

### Notes

- Requires Dispatcharr 0.32.0 or newer. Publication and native acceptance are
  pending.

## [0.6.4] - Prerelease candidate

### Fixed

- Complete a pending return to an allowed source after its first committed
  archive media, including recovery when the acknowledgement was committed but
  its response was lost. The supervisor completes that acknowledgement before
  observing viewers again, allowing a fresh attach in the same pass.
- Preserve idempotency when a media callback races with supervisor completion;
  retries remain bound to the same recorder attempt, lease and applied source.

### Notes

- Requires Dispatcharr 0.32.0 or newer. Publication and native acceptance are
  pending.

## [0.6.3] - In-place presentation refresh

- Adds read-only Current recordings and Today's recording schedule plugin actions.
- Schedule overview uses the applied schedule and timezone, shows current,
  upcoming and past windows, and marks a global recorder pause.
- Refreshes both action messages in place with compact scheduled-channel rows;
  structured diagnostic and schedule data remain available to callers. This
  presentation update requires no configuration migration or archive change.
- Current-recording rows omit channel numbers and use only the first word of
  the validated M3U profile name.
- Sizes the nonbreaking-space gap between visible rows from static 17px Lato
  measurements calibrated to a 350px row. This is a width estimate, not a
  runtime viewport measurement. Copied text may retain nonbreaking spaces,
  which can be replaced with ordinary spaces in the destination.
- A manually dispatched CI run can explicitly skip native AIO for package/unit
  validation only; pull-request defaults and release evidence requirements are
  unchanged.

## [0.6.2] - Prerelease candidate

### Fixed

- Continue authenticated catch-up playback across programme changes without
  requiring later EPG entries. M3U/XMLTV HLS playlists follow newly indexed
  segments up to a 24-hour session bound; XC streams follow contiguous indexed
  segments within Dispatcharr's duration limit.
- Keep live-edge waiting finite and recorder-aware. XC waits at most 125 seconds
  (the recorder's 120-second media-idle timeout plus five seconds for indexing
  and finalization), stops at a real archive gap, and ends promptly when its
  recorder lease is absent. HLS uses the same edge freshness window.

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
