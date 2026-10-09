# Validation status

## Released baseline: 0.1.4

M3U/XMLTV with HLS passed qualitative real-player acceptance on Dispatcharr
0.32.0: start-over, repeated forward/backward seeking, pause/resume and a
programme transition. No consistent EPG-to-playback drift was reported.
Client-managed continuation requested a new archive window at the boundary;
the previous EVENT playlist stayed bounded to its programme.

These results do not establish exact displayed-frame timing or automatic
continuation in other clients. Native XC real-player acceptance, extended
retention and rollback playback checks remain open. Personal channel,
programme, player and deployment details are not published.

## Automated release evidence

Version 0.1.4 passed 157 local tests, Ruff, compileall, package build,
independent review and all three required CI checks. Disposable, networkless
AIO integration tests passed with the pinned Dispatcharr 0.31.0 and 0.32.0
images. Those probes used the bundled FFmpeg and synthetic media, including
two audio tracks and an unsupported private data PID.

The AIO probes cover plugin loading, route idempotence, M3U/XMLTV export,
HLS playback, XC minute-based duration, UTC timestamps, timestamp seeks,
user permissions, invalid credentials, the catch-up switch and byte ranges.
Synthetic HTTP checks include 206 responses with matching Content-Range;
unauthenticated requests are rejected. These checks do not establish player
compatibility by themselves.

Run before publishing a package:

- `python3 -m unittest discover -s tests -q`
- `python3 -m compileall -q catchuparr scripts tests`
- `ruff check .`
- `python3 scripts/build_plugin.py`
- Both pinned AIO integration jobs in CI.

CI uses Python 3.12 and Ruff 0.13.3. Package assets contain no operational
backups, personal recordings or diagnostic exports.

## Programme boundaries and seeking

A reproduced request pattern changed the requested UTC instant on seeking
while retaining the original programme duration. Using that duration without
an EPG boundary could include the following programme. Local M3U windows now
resolve the instant to the effective programme and stop at its end. Historical
programme snapshots require archive coverage; missing guide data returns 404.

Tail reads may be prefetches and do not authorize extension into a following
programme. HLS reloads may append newly completed segments within the selected
programme. A crossing segment is omitted. The 60-second playlist target
accommodates delayed keyframes, including a tested 16-second segment.

Sanitized server diagnostics showed successful requests with no playlist
extending past its EPG programme end and first-segment alignment within one
segment of the requested instant. This is segment-index evidence, not an
instrumented measurement of the first displayed frame. Earlier checks found
continuous archive segments and matching indexed UTC times; no programme-index
shift was established.

A reproducible real-player regression should start available programmes in
chronological order, repeat each start, seek forward and backward at least
three times, pause/resume and test a natural transition separately. Measure
requested time against visible content when checking the two-segment tolerance.
A 404 for unavailable material is not a successful start-over. Keep identifying
traces and device details private.

## Other regressions and limits

- Local XC playback uses the requested minute interval, or the actual EPG end
  when no valid duration is supplied. Provider duration padding remains with
  the provider path. Both AIO versions passed completed-minute fixtures.
- XC-first admission migrates legacy HLS session schemas. Malformed schemas
  reject admission and clean pending rows.
- Session checks cover programme switches, grace periods, HEAD requests during
  playback and rejection of another credential under a one-stream limit.
- Recorder acquisition uses the archive's durable fence after Redis restarts
  and releases leases on setup failure. An isolated restart resumed indexing
  without manual fence repair.
- Native XC M3U annotation does not restore historical guide entries through
  the core XMLTV endpoint. Use the plugin M3U/XMLTV pairing for local history.

## Development isolation and diagnostics

Use one isolated AIO container with separate data and archive storage, blocked
outgoing connections except explicitly allowed test sources, and initially
disabled imported providers/jobs. The backup-ZIP/API restore procedure is in
[development and deployment](development.md). Verify anonymous denial,
authenticated playback and recorder progress after updates. Production changes
are outside this validation scope.

Enable sanitized diagnostics only for a test and disable them afterward.
Review collected output before sharing; never publish raw URLs, session tokens,
personal EPG data or real-world request timestamps.

## 0.2.1 YAML filter engine (included in the 0.3.0 candidate)

The unified YAML engine replaced the unreleased 0.2.0 configuration interface.
Its implementation passed 232 local unit tests, Ruff, compileall and package
build. Independent review covered the YAML schema, enabled Channel Profile
memberships, source-policy enforcement, snapshot validation, automatic legacy
filter reset and isolated task tests.

Fresh synthetic integration passed in both pinned Dispatcharr 0.31.0 and
0.32.0 AIO images. The probes covered catalog extraction, atomic Apply,
source ranking, native pool accounting, private recorder media, fenced
cross-process reuse, concurrent native live-stream isolation, recorder startup,
capacity and runtime failover, indexed media gaps and connection cleanup.
Native profile creation automatically supplies memberships; fixtures disable
these before enabling their intended synthetic members.

The engine is included in 0.3.0 below. The earlier 0.2.1 package was never
released and has no separate player-acceptance claim.

## Historical 0.2.0 candidate evidence (unreleased)

Source-policy enforcement is an unreleased candidate. The combined server-side
AIO checks passed in both pinned Dispatcharr versions; real-player regression
remains an open release gate. The configuration and native-helper probes
passed in both pinned AIO versions:
real assigned-source catalog extraction, atomic Apply/read-back, include-policy
ranking, native profile capacity and reservation-ledger teardown with duplicate
release protection. The DRF compatibility gate passed after independent review
of its registered-route association and captured original handler signature.

The single-process native media probe also passed in both images. It used
synthetic HTTP sources and the actual private recorder route to decode video and
audio from the included source, reject forbidden/unassigned sources and forged
capabilities, preserve native assignment keys, proxy an actual Redirect default
profile without returning Location, and release the reserved slot exactly once.

The cross-process probe passed in both images. A fresh process attached to the
same private worker without opening another provider connection or reserving
another slot. After that client closed, the original client consumed decoded
media published beyond the recorded Redis head. Native ownership, active state,
client counts and the running upstream manager remained valid; final teardown
returned provider capacity to its baseline.

Seeded credential counters do not prove provider credential acquisition.
The recording-engine probe also passed in both images using the private HTTP
route, actual recording task and bundled FFmpeg. It verified fallback when the
first source supplied no useful media, fallback when that source was at capacity,
and a later source change after useful recording stalled. Decoded synthetic
audio identified archived sources independently of remuxed service names. The
excluded source was absent, existing recordings survived the change, and
teardown restored provider capacity. The probe also checked the resulting
gap/discontinuity. Separate synthetic stream-copy checks covered short segments
and rejection of unknown fingerprints.

The native live/archive isolation probe passed in both images. Without source
rules, recording shared the existing live connection. With a restricted source
policy, a separate worker recorded the permitted source while the original live
client continued yielding newly published media from its original source.
Channel assignments and live-source metadata stayed unchanged. Closing the
archive recorder left the live stream active; closing its final client then
ran native disconnect cleanup and restored provider capacity. These checks used
synthetic sources and the actual native routes and recording task.

Dev real-player regression remains an acceptance gate.
Passing parser or configuration tests alone is insufficient.

Safe reuse of an existing live worker with restrictive source rules has not
been verified. Current source metadata and a fresh buffer cursor cannot prove
chunk origin across source transitions or in-flight writes. The existing shared
route remains the default without rules; the implemented override path uses a
dedicated worker subject to native provider capacity. This dedicated-connection
behavior is the approved 0.2.0 candidate scope; safe reuse of native live
workers remains deferred. See the
[roadmap](roadmap.md) for the remaining acceptance requirements.

## 0.3.0 schedule and recorder-control candidate (unreleased)

The 0.3.0 manifest and package candidate add weekly recording windows, a durable
global recording pause/resume control, configuration-generation fencing and
scoped `[Catchuparr]` logs. The full synthetic native integration probe passed
on pinned Dispatcharr 0.31.0 and 0.32.0. Both runs covered schedule validation
and application, pause/resume while recording, natural schedule closure, stale
queued-job rejection, restricted-source recording and failover, preservation
of a concurrent native live stream, useful-media/gap checks, reservation
release and native worker teardown. Synthetic sources and media were used.

The local suite passed 299 tests after the playback-grace test was changed to a
shared fake clock. Ruff, compileall and package-build checks passed. Required CI
and real-player acceptance remain open; these server-side tests do not establish
EPG or player behavior. The candidate is not published. See the
[0.3.0 candidate guide](candidate-0.3.0.md) for configuration and upgrade
behavior.
