# Catchuparr

Local catch-up and start-over archive for Dispatcharr. Development is in progress.

The repository contains an installable Dispatcharr plugin, synthetic-stream tests,
and an isolated development stack. Production configuration and recordings are not
stored here.

The supported Dispatcharr versions are 0.31.0 and 0.32.0. The first client path
is M3U plus XMLTV with HLS archive playback. Dispatcharr remains the live proxy
and EPG source. The XC adapter adds local TS archive playback and native XC M3U
metadata through guarded, reversible hooks. TiviMate 5.3.3 on Shield TV has
played the selected Dev channel from the start of its current programme, with
pause and seeking, through the M3U/XMLTV path.

Repeat player tests after upgrades; native XC player validation is separate.

See [installation and development](docs/development.md) and
[validation status](docs/validation.md).
