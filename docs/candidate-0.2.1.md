# 0.2.1 YAML filter candidate

This is an unreleased implementation target. Required CI, native integration
and isolated real-player acceptance must pass before publication.

## Configuration

Use the single **Filter configuration (YAML)** field. Channel UUIDs are internal
only. A blank document selects no channels. Validate lists every planned
recorder; Apply activates the resolved selection and policies atomically.
Saving a draft never changes recording. There is no migration from old settings
or snapshots. On startup, the plugin detects and deletes pre-0.2.1 filter
settings and active snapshots, leaving an empty YAML configuration. General
archive/storage settings, recordings and playback credentials are preserved.
The reset is idempotent and no recorder starts before a valid Apply.

```yaml
version: 1
profile: all
rules:
  - channels:
      profile: all
    include: ["Primary M3U", "Backup M3U"]
    priority:
      "Primary M3U": 100
      "Backup M3U": 50
  - channels:
      numbers: [3, "10-12"]
    exclude: ["Excluded M3U"]
    priority:
      "Preferred M3U": 100
```

The outer `profile` defaults to `all` and limits the eligible channels. A named
Channel Profile includes its enabled members only. Each `channels` mapping
contains exactly one of `numbers`, `names`, `groups` (lists), or `profile`
(one name or `all`). Quote decimal numbers and ranges for exact representation.
Unknown or ambiguous selectors and nonempty rules with no matches are errors.

`profile: all` explicitly selects all eligible channels and supplies a default
policy. A specific rule replaces that entire policy, including priorities.
Other rule overlaps are errors. `*` and the old text syntax are not supported.
Apply freezes the internal selection; numbering, new channels and profile
membership changes require another Apply.

`include` and `exclude` cannot coexist. Include requires a nonempty M3U-name
list. `exclude: []` permits every assigned source. Priorities are integer scores
allowed only with include or exclude: filtering precedes descending priority;
missing scores are zero and equal scores preserve channel stream order. Without
priorities, preserve channel stream order. A priority for a forbidden source is
an error. Account names must be unique exact matches; no streams are auto-mapped.

With only a channel selector, or `exclude: []` without priority, recording uses
the shared live route. Effective filters or priorities use a dedicated native
worker and may need additional provider/tuner capacity. Fallback never leaves
the allowed sources. The storage limit applies to the whole plugin archive.

Duplicate/unknown keys, invalid types, custom tags, anchors, aliases and merges
are rejected. The document is limited to 64 KiB. Validation reports rule/field
and, where available, line information. Errors leave active settings intact.

## Install, reset and rollback

Build with the documented unit, compile, lint and package commands. Import
`dist/catchuparr-0.2.1.zip` through Dispatcharr with overwrite enabled, then
restart the isolated AIO so web and workers load the same version. Stop old
recorders before installation. The plugin removes detected legacy filter
configuration and requires a new Validate/Apply. Preserve the
archive path and general storage/retention settings; do not delete recordings.

Use the designated operator for backups, installation and container start/stop/restart. Back up isolated application data and configuration only;
exclude recordings and the archive database unless explicitly requested. Keep
at most two verified task-owned Dev backups, pruning older backups only after
verification. Production and unrelated backups are outside this policy.

For rollback, stop candidate recorders, reinstall the saved previous package
and restore its matching application settings/snapshot before restarting.
Existing archives stay in place. A data-only backup cannot restore overwritten
archive content. Check status, recording progress and authenticated playback.

## Acceptance

Require green unit/lint/package checks and synthetic native integration in both
pinned AIO versions, covering profiles, selection, Apply, source order, capacity,
fencing, playback and cleanup. Then verify actual Dev recording selection,
source use, start-over, seeking, pause/resume and programme transition with a
real player. Server-side success does not replace player acceptance.
