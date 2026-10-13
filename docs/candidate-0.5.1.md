# Catchuparr 0.5.1 prerelease candidate

Version 0.5.1 adds inherited source filters and recording schedules for named
channel profiles. It preserves Dispatcharr's enabled profile membership. The
candidate is not published. Its 453-test suite and full pinned AIO probes
passed on Dispatcharr 0.31.0 and 0.32.0. Required CI, isolated Dev
verification and publication remain pending.

## Configuration behavior

- A named `channels.profile` rule provides a base for its enabled members.
  Direct `channels.names` and `channels.numbers` rules can provide exceptions
  regardless of YAML order; group rules cannot act as profile exceptions. The
  `channels.profile: all` rule remains a fallback base. Multiple matching
  direct rules or competing named-profile rules fail validation.
- An omitted `include` and `exclude` inherits the matching base filter. An
  explicit `include` or `exclude` replaces it; those fields remain mutually
  exclusive. An explicit `priority` replaces the complete inherited map; an
  omitted map inherits, and `priority: {}` clears inherited scores. The final
  combined policy is validated at Apply.
- An omitted schedule inherits. A rule with its own schedule uses whole-week
  replacement by default, and unspecified days are off. Use
  `schedule_mode: replace` to state this explicitly, or
  `schedule_mode: override_days` to replace only specified days after group
  expansion. All other days inherit; an explicitly empty day is off.
- `schedule_mode` requires a `schedule` on the same rule. Expand groups before
  inheritance, with precedence `weekday > group > daily`. Replacing a start day
  removes its inherited overnight windows; windows starting on the previous day
  still extend into it.
- Validate previews show the effective filter, priority map and schedule with
  their origins. Apply validates and stores those same effective values and
  origins in one configuration generation. Existing applied snapshots from
  versions 2, 3 and 4 remain readable.

See [the main configuration guide](../README.md#named-profile-inheritance-in-the-051-prerelease)
for complete synthetic YAML examples, including invalid overlapping rules.

## Release state

The local package version is 0.5.1. The 453-test suite and full pinned AIO
probes passed on Dispatcharr 0.31.0 and 0.32.0. Required CI, isolated Dev verification and publication remain
pending. An initial 0.32.0 short-media attempt failed before 96,256 accepted
bytes; a later diagnostic-only run passed, but the original cause remains
unknown. The candidate has not been installed in isolated Dev or published.
