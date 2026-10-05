# Validation status

## Automated checks

Run `python3 -m unittest discover -s tests -q`,
`python3 -m compileall -q catchuparr scripts tests`,
`ruff check .`, and `python3 scripts/build_plugin.py` before publishing a ZIP.
CI runs the same checks with Python 3.12 and Ruff 0.13.3. Unit and synthetic
stream tests do not establish player compatibility.

## Isolated Dispatcharr Dev stack

The current Dev stack uses one Dispatcharr v0.31.0 AIO container with separate
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

TiviMate 5.3.3 on Shield TV still needs to establish catch-up recognition,
start-over during recording, repeated forward and backward seeks, pause and
resume, programme change, restart and retention behavior. Record request
methods, time arguments, Range headers, status codes and observed playback
without recording bearer tokens. XC must be tested separately before its
hooks are enabled.

On 2026-10-06, the Dev AIO was rebound from loopback to the host's LAN
interface for the Shield test. Its mounts and restricted Vu+ egress rule were
rechecked. An unauthenticated LAN request returned HTTP 401; a separate
device token fetched M3U and XMLTV successfully. The token and complete
playlist URLs are stored only in a private local file outside this repository.
