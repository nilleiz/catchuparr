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

The plugin M3U/XMLTV endpoint and direct XC JSON are the two primary output
paths. Native `/get.php` playlist annotation is tested separately; its core
`/xmltv.php` guide has not been extended to restore local archive history.
Historical guide visibility with that particular pairing remains unverified.
