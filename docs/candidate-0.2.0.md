# 0.2.0 candidate: install, source rules, and rollback

This is an **unreleased candidate** for isolated evaluation. It is not a
published or player-validated release. The server-side integration probes pass
on the two supported Dispatcharr versions; real-player regression is still
required before release.

## Install the candidate in an isolated instance

Build the plugin ZIP from the repository:

```sh
python3 -m unittest discover -s tests -q
python3 -m compileall -q catchuparr scripts tests
ruff check .
python3 scripts/build_plugin.py
```

The build creates `dist/catchuparr-0.2.0.zip`. Keep the previous plugin ZIP and
a restorable snapshot of the isolated instance's settings and archive storage.
Stop active recorders before updating. Import the ZIP with overwrite enabled,
reload plugin discovery, then restart the instance so web and worker processes
load the same plugin files. Check that the plugin reports version 0.2.0, passes
its compatibility check, and reports archive status before testing recording
or playback.

The candidate keeps the existing `channel_uuids` setting as the recording
selection. `source_rules` is a draft until **Apply configuration** succeeds.
Use **Validate** to review resolved channels and candidate source order first;
Apply activates the validated configuration atomically. A failed validation or
Apply leaves the previous active configuration in place. Saving settings alone
does not activate new rules.

## Source rules

Use one of these forms for each non-empty line:

```text
<selector> | mode=include-only | m3u="Exact Account Name"
<selector> | mode=priority | priority="Exact Account Name":100
```

Selectors are `*` for the global rule, `number:` for channel numbers and
inclusive ranges, `name:"..."` for an exact channel name, or `group:"..."` for
an exact group. For example:

```text
* | mode=priority | priority="Primary M3U":100,"Backup M3U":50
name:"Synthetic Channel A" | mode=include-only | m3u="Primary M3U"
number:3,10-12 | mode=exclude-only | m3u="Backup M3U"
group:"Synthetic Group A" | mode=unchanged
```

The modes are:

- `include-only` permits only the named assigned M3U accounts.
- `exclude-only` permits assigned accounts except those named.
- `priority` ranks every currently assigned account. Higher scores rank first;
  an account without an explicit score gets zero. Equal scores retain the
  existing Dispatcharr assignment order.
- `unchanged` uses the normal shared live stream for the selected channels.

Account names must match exactly and be unique. Rules can only select streams
already assigned to a channel; they do not add mappings or change assignment
order. A channel-specific rule replaces the global rule for that channel.
Overlapping specific selectors, unknown or ambiguous names, and invalid rules
are rejected during validation.

With `source_rules` blank, recordings continue to share the ordinary live
channel stream. An override starts a dedicated worker and may consume another
provider or tuner slot. Check provider capacity before applying restrictive
rules. If an allowed source cannot start or stops providing useful media, the
recorder can try the next permitted source. It never falls back outside an
`include-only` list. After starting, it stays with the selected source until a
failure, a new recording window, or a newly applied configuration.

## Upgrade and rollback

For an upgrade, stop recorders, preserve the current plugin ZIP and a matching
instance snapshot, then import the candidate with overwrite enabled. Reload
plugin discovery and restart the instance before resuming recording. Apply a
source policy only after Validate shows the intended channels and ordered
sources. Preserve the snapshot until recording and playback checks complete.

To roll back, stop recorders, reinstall the saved plugin ZIP, reload plugins,
and restart all processes. Restore the pre-upgrade plugin settings so the old
version does not retain candidate-only policy values. Verify plugin status and
archive playback. If the old package does not pass those checks, restore the
matching pre-upgrade instance and archive snapshot together. Keep the archive
storage when its checks pass; do not delete it as part of a package rollback.

## Release gate

Both pinned server-side AIO integration probes pass, including assigned-source
isolation, authenticated recorder media, fallback, cleanup and preservation of
an already active live stream. These checks do not replace real-player
regression. Start-over, seeking, pause/resume and a programme transition must
pass with a real player before 0.2.0 can be published. See the
[validation status](validation.md) and [roadmap](roadmap.md).
