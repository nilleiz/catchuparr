# Catchuparr roadmap

Baseline: released 0.1.4, M3U/XMLTV + HLS real-player acceptance completed with Dispatcharr 0.32.0. Production deployment is outside this roadmap. Each wishlist item gets its own release, ordered from hardest to easiest.

## Workflow and release gates

- Implementation: **gpt-6-luna**, reasoning appropriate to complexity.
- Analysis, independent reviews and technical acceptance: **gpt-6.1-sol**, reasoning **low through high**.
- Use separate worktrees, small `develop/` feature branches and PRs. Merge after independent review and green required checks; fix failures without weakening checks.
- Every release includes an installable plugin ZIP, SHA-256 checksum, release notes and installation/update/rollback guidance.
- Relevant synthetic tests, lint, compile and package checks, plus integration probes in pinned Dispatcharr 0.31.0 and 0.32.0 AIOs, gate publication. New private hooks are version/signature checked, idempotent and removable.
- Deploy only to the isolated single-container Dev AIO, with its separate database, Redis, volumes, ports and restricted egress. Preserve consistent private backups and existing archives.
- Recording changes require real-player regression for start-over, seeking, pause/resume and programme transition. CI does not replace real-player acceptance.
- No companion container is planned; no production update or upstream PR is authorized by this roadmap.

## Release sequence

| Release | Feature | Relative effort | Status |
| --- | --- | --- | --- |
| 0.2.0 | M3U include/exclude rules and fixed priorities | High | In progress |
| 0.3.0 | Catch-up playback in Dispatcharr Stats | High to medium | Planned |
| 0.4.0 | Recording windows by channel and weekday | Medium | Planned |
| 0.5.0 | Hide archive recorders in Stats | Medium to low | Planned |
| 0.6.0 | Select channels by number, range, name or group | Low | Planned |

Shared rule parsing and the internal channel resolver begin in 0.2.0. The complete replacement of the recording channel selection UI ships in 0.6.0, rather than an additional foundation release.

## Settings contract

All parameters are configured in the native Plugin Settings dialog using switches, selections and readable text rules with examples. Add **Validate/Preview** and **Apply** actions. Saving a draft does not activate unvalidated rules. Apply validates the whole applicable configuration and activates it atomically; errors preserve the last active configuration.

Preview lists resolved channels, permitted source order, recording windows, conflicts and possible additional source connections. UUIDs remain internal stable archive identities. Existing settings migrate compatibly; absent new rules preserve the existing behavior.

## 0.2.0 — source rules

### Behavior

- Consider only streams already assigned to the Dispatcharr channel; do not discover or auto-map unassigned streams.
- Modes: unchanged, `include-only`, `exclude-only`, and priority only. Reference M3Us by unique name.
- Higher priority wins. Missing priority is zero; equal priorities retain `ChannelStream` order. Weighting is deterministic preference, not random distribution.
- Permit a global rule. A channel-specific rule replaces the global policy completely. Reject overlapping channel-specific rules, unknown or ambiguous M3U names and invalid syntax.
- With no override, retain the existing shared channel proxy. With an override, use a dedicated proxy worker and respect native provider capacity. This release scope was explicitly approved after independent review found that existing live buffers lack verified source provenance. Additional provider connections or source tuners may be required. Safe reuse of existing native live workers is deferred; reconnecting to the same managed recorder worker is a separate lifecycle case.
- Do not modify channel membership/order, the live channel's Redis assignment, or the live worker's source. Reuse Dispatcharr profile reservation and slot release, including Redirect sources through a narrowly authenticated internal recorder/proxy adapter.
- On connection exhaustion or failure, try the next permitted source. Never escape an include list. After successful fallback remain on that source until failure, a new recording window or applying new rules.
- Preserve copy recording with all audio tracks, segment indexing and session-protected retention. Report unavailable sources as recorder status and real archive gaps.

### Rules

Use `<selector> | mode=<mode> | m3u="Name","Other" | priority="Name":100,"Other":50`. `*` selects the global default. Selectors use `number:1,3,10-20`, `name:"Synthetic Channel A"` or `group:"News"`. The parser respects quoted names. Include/exclude lists apply before ranking.

### Acceptance

Synthetic tests cover filtering, deterministic ties, blocked profiles, failed starts, source failover and release of reservations. Both pinned AIOs must verify ordinary shared recording and isolated override workers, including Redirect sources. In Dev, a channel playing live from one source can archive from another without changing live playback. Agree real test sources/tuner availability before enabling additional connections. Real-player playback regression must pass before publishing 0.2.0.

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

## 0.6.0 — channel selection

### Behavior

- Replace the normal UUID input with channel numbers, inclusive ranges, decimal channel numbers, exact names and groups.
- Examples: `number:1,3,10-20`, `name:"Synthetic Channel A"`, `group:"Synthetic Group A"`.
- Union selection lines and deduplicate channels. Reject unknown/ambiguous identifiers and malformed ranges in preview.
- Apply resolves to a stable UUID snapshot. Later renumbering or group changes require another Apply; no automatic unexpected recordings.
- Preserve old UUID settings through compatible migration and retain every existing archive under its internal channel identity.

### Acceptance

Test ranges, decimal numbers, ambiguous names/groups, duplicates, channel removal/renumbering, changed group membership, migration and the actual recorder selection against preview. No archive resets or implicit all-channel fallback.

## Deferred validation

Native XC real-player acceptance remains separate from the completed 0.1.4 M3U/HLS milestone. Automatic continuation in other clients, exact displayed frame timing and extended retention/rollback playback checks are not yet established. Releases must describe actual evidence rather than infer player compatibility from server tests.
