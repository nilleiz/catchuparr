# Catchuparr

Local catch-up and start-over archive for Dispatcharr. Development is in progress.

The repository contains an installable Dispatcharr plugin, synthetic-stream tests,
and an isolated development stack. Production configuration and recordings are not
stored here.

The first client path is M3U plus XMLTV with HLS archive playback. Dispatcharr
remains the live proxy and EPG source. The XC integration is still under
development and is not enabled by the plugin. TiviMate 5.3.3 on Shield TV has
played the selected Dev channel from the start of its current programme, with
pause and seeking, through the M3U/XMLTV path.

See [installation and development](docs/development.md) and
[validation status](docs/validation.md).
