# Catchuparr 0.7.0 prerelease candidate

Native M3U catch-up now exports an `archive.m3u8` resource with `utc` and
`lutc`. Clients need not substitute a duration. Existing native URLs accept
numeric duration seconds, known unexpanded duration placeholders, or no
length. Guide end is preferred for an omitted length; otherwise a bounded
snapshot is used. `lutc` denotes the client clock, not the programme end.
Unknown malformed parameters and conflicting bounds remain errors.

Historical native requests return closed HLS playlists. The legacy standalone
route remains compatible with its existing continuous contract. Explicit
`playback=continuous` opts into continuous native or local XC playback.

XC path/query durations are minutes. Local XC starts only with archived media
at the requested instant. Missing later media no longer forces an unrelated
provider fallback: the response ends at the first gap, discontinuity, or current
archive end. `X-Catchuparr-Archive-Partial: true` and
`X-Catchuparr-Archive-Stop: gap-or-end` identify incomplete responses. This does
not reconstruct missing media, concatenate across gaps, or substitute live TV.
Clients may not display these headers; a partial programme will end early.

Authentication, capability binding, network and channel access policies,
stream limits, and signed segment checks remain active. A missing local start
retains the existing provider fallback.

## Acceptance

Focused synthetic tests cover partial substitution, malformed and duplicate
parameters, bounded historical media, finite XC durations, gap handling and
missing starts. Required native CI runs against Dispatcharr 0.32.0 and 0.33.0.
Actual external-player playback remains a separate acceptance step.
