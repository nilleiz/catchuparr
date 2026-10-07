# Validation status

## Automated checks

Run `python3 -m unittest discover -s tests -q`,
`python3 -m compileall -q catchuparr scripts tests`,
`ruff check .`, and `python3 scripts/build_plugin.py` before publishing a ZIP.
CI runs the same checks with Python 3.12 and Ruff 0.13.3. Unit and synthetic
stream tests do not establish player compatibility.

## Isolated Dispatcharr Dev stack

The current Dev stack uses one Dispatcharr v0.32.0 AIO container with separate
data, archive, Redis, database, network and port paths. The Dev egress rule
permits the selected Vu+ endpoint only. The copied providers, scheduled jobs,
recording rules and integrations were disabled before testing. The selected
test channel is **Das Erste HD**; other imported channels are not recorded.
This Dev instance predates the backup-ZIP/API restore procedure now documented
for future rebuilds; it was seeded from an isolated, scrubbed database copy.

On 2026-10-05, the Dev recorder was observed adding short transport-stream
segments continuously after an FFmpeg data-PID mapping fix. A non-admin Dev
user received an M3U containing the selected channel's catch-up attributes
and an XMLTV document containing its current programme. The authenticated
archive route returned an appendable HLS event playlist and a real segment;
`Range: bytes=188-563` returned HTTP 206, the corresponding `Content-Range`,
and 376 bytes. These HTTP checks used a token in a private request header.

Programme change, service restart and retention behavior still need a
real-player check. Record request methods, time arguments, Range headers,
status codes and observed playback without recording bearer tokens. XC must
be tested separately before production use.

On 2026-10-06, the Dev AIO was rebound from loopback to the host's LAN
interface for the Shield test. Its mounts and restricted Vu+ egress rule were
rechecked. An unauthenticated LAN request returned HTTP 401; a separate
device token fetched M3U and XMLTV successfully. The token and complete
playlist URLs are stored only in a private local file outside this repository.
The user then confirmed on TiviMate 5.3.3 running on Shield TV that the
M3U/XMLTV setup, start-over from the beginning of the current programme,
pause and seeking all work for **Das Erste HD** with the plugin's
`catchup="default"` and `{utc}`/`{duration}` URL template. A request trace
and separate results for programme changes, restarts, retention and XC remain
open validation items.

## Dispatcharr 0.32.0 upgrade (2026-10-07)

The isolated Dev data and archive were backed up cold before changing the image.
Dev now uses the pinned official 0.32.0 AIO with PostgreSQL 17 and the same
isolated mounts, port and restricted source network. Production was not updated.
The plugin was imported through the authenticated administrator API, then Dev
was restarted so web and workers load the same package. Only Catchuparr's two
schedules and the selected test provider were enabled; provider EPG refreshes
remained disabled. A recorder lease and newly indexed segments were observed.

Authenticated M3U and XMLTV requests returned 200; an anonymous playlist request
returned 401. Local entries include UTC metadata while preserving the tested
seconds-based URL template. Separate disposable AIOs passed synthetic HLS,
authorization and Range checks for both 0.31.0 and 0.32.0. Native XC probes also
exercise minute-based duration, integer UTC epoch values, timestamp seeks,
channel permissions, invalid credentials and the user catch-up switch. Their
checks passed in both pinned AIO images on the final implementation. Each AIO
also passed 21 archive/playlist/recorder tests using its bundled FFmpeg, including
two audio tracks and an unsupported private data PID. The final combined suite
passed 131 tests, Ruff, compileall and package build. An independent review
reproduced and verified session reuse, a 48-hour programme switch, HEAD requests
during an open stream and second-device rejection under a one-stream limit.

The final Dev package loaded all four guarded XC hooks. Authenticated playlist
access remained successful, and the selected recorder continued indexing new
segments after the final restart. The original bootstrap, HLS integration and
compatibility PRs were merged only after review and green checks. The final XC
and AIO-CI PR requires both image integration jobs and the package job to pass.

The user will repeat the Shield/TiviMate tests later. The earlier successful
0.31.0 player results do not establish 0.32.0 or XC player compatibility. A
stable release remains gated on those real-player checks; CI plugin ZIPs are
development artifacts.

An additional historical Dev request exposed Dispatcharr's five-minute provider
duration padding crossing a later archive gap. Local playback must use the
exact requested minute interval, or the actual EPG end when no valid duration
is supplied. Provider playback retains Dispatcharr's duration handling. A
completed-minute fixture in both AIO integration jobs covers this regression.
Both versions passed that fixture with an explicit duration and with the real
EPG helper supplying the end. A subsequent Dev XC-first request exposed a
legacy HLS-session schema that had not yet been migrated; the integration
fixture now starts from that schema and exercises XC before HLS.
The completed fix passed 136 local tests, Ruff, compileall, package build and
both pinned AIO probes (including the bundled FFmpeg tests). Independent review
also verified that malformed session schemas reject admission and clean up the
pending admission row. After reinstalling and restarting the Dev AIO, an actual
historical one-minute request returned HTTP 206 with the requested 188 TS bytes
and `Content-Range: bytes 0-187/...`. No duration-padding or legacy-schema
workaround was needed for that request.

The plugin M3U/XMLTV endpoint and direct XC JSON are the two primary output
paths. Native `/get.php` playlist annotation is tested separately; its core
`/xmltv.php` guide has not been extended to restore local archive history.
Historical guide visibility with that particular pairing remains unverified.

## TiviMate seek regression (2026-10-07)

The user confirmed that first start-over of the current programme works, but
bar seeks can land minutes away from the selected point. Sanitized Dev access
records show TiviMate 5.3.3 changes `{utc}` on each seek while leaving the
original programme's `{duration}` at 2700 seconds. For a programme ending at
16:00 UTC, one later request started at 15:47 UTC; the resulting 45-minute
window contained 58 segments from the next programme. This is a reproducible
server-side programme-boundary error. Version 0.1.2 bounds every M3U seek by
its actual EPG programme, including historical programmes restored from the
local XMLTV snapshots. A missing programme still returns 404. The sanitized
archive trace now uses WARNING so it appears in the Dev AIO logs when enabled.

The inspected 1026 indexed segments were continuous, with durations from 4.8
to 7.02 seconds. The user subsequently reproduced a more serious symptom:
selecting each of several programmes started the *following* programme. The
sampled archive frames matched their indexed UTC times, so a one-programme
index shift was not found. Tail segment reads may be player prefetches;
allowing them to extend the HLS session into the next programme is therefore
unsafe. Version 0.1.3 confines each session to the chosen programme and will
be checked on Shield before release. Automatic continuation across programme
boundaries remains disabled. The fixed 60-second target remains because FFmpeg
can emit a 16-second segment when the next keyframe is delayed. TiviMate must
still confirm that start-over and seeks land within two segments. A separate
mechanism and player test are needed for automatic cross-programme playback.

For the next Shield test, use the unchanged private M3U/XMLTV URLs and refresh
both exports. Choose programmes whose full start is still inside the available
archive. Start three programmes in chronological order, note the first visible
content, then repeat the same starts. For one programme, seek forwards and
backwards at least three times and pause/resume. Record the selected time and
visible time; an error greater than two archived segments fails acceptance.
The server trace must show HTTP 200 for the requested programme and segments
within that programme. A 404 indicates unavailable material and must not be
counted as a successful start-over. Test automatic continuation separately
after its replacement mechanism is implemented.
