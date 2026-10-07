# Catchuparr

Local catch-up and start-over archive for Dispatcharr.

The repository contains an installable Dispatcharr plugin, synthetic-stream tests,
and an isolated development stack. Production configuration and recordings are not
stored here.

The supported Dispatcharr versions are 0.31.0 and 0.32.0. The first client path
is M3U plus XMLTV with HLS archive playback. Dispatcharr remains the live proxy
and EPG source. The XC adapter adds local TS archive playback and native XC M3U
metadata through guarded, reversible hooks. TiviMate 5.3.3 on Shield TV has
validated start-over, forward/backward seeking and pause/resume for Das Erste HD
through the M3U/XMLTV path on the isolated Dispatcharr 0.32.0 Dev stack.
The user also observed automatic playback from Tagesthemen into Maischberger;
TiviMate requests a new archive window at the programme boundary. This client
behavior is not a guarantee of automatic continuation in other players.

Repeat player tests after upgrades; native XC player validation is separate.

See the [release roadmap](docs/roadmap.md), [installation and development](docs/development.md) and
[validation status](docs/validation.md). The M3U/HLS milestone is complete in
[0.1.4](https://github.com/nilleiz/catchuparr/releases/tag/v0.1.4); XC player
validation remains open.
