# Catchuparr

Local catch-up and start-over archive for Dispatcharr. Development is in progress.

The repository contains an installable Dispatcharr plugin, synthetic-stream tests,
and an isolated development stack. Production configuration and recordings are not
stored here.

The first client path is M3U plus XMLTV with HLS archive playback. Dispatcharr
remains the live proxy and EPG source. The XC integration is still under
development and is not enabled by the plugin. TiviMate playback and seeking on
the target Shield TV have not yet been verified.

See [installation and development](docs/development.md) and
[validation status](docs/validation.md).
