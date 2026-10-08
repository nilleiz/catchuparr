# Catchuparr

Local catch-up and start-over archive for Dispatcharr.

The repository contains an installable Dispatcharr plugin, synthetic-stream tests,
and an isolated development stack. Production configuration and recordings are not
stored here.

The supported Dispatcharr versions are 0.31.0 and 0.32.0. The first client path
is M3U plus XMLTV with HLS archive playback. Dispatcharr remains the live proxy
and EPG source. The XC adapter adds local TS archive playback and native XC M3U
metadata through guarded, reversible hooks. Real-player checks confirmed
start-over, forward/backward seeking, pause/resume and client-managed programme
transitions through M3U/XMLTV on Dispatcharr 0.32.0. This evidence is qualitative;
exact displayed-frame timing and automatic continuation in other clients remain
unverified.

Repeat player tests after upgrades; native XC player validation is separate.

See the [release roadmap](docs/roadmap.md), [installation and development](docs/development.md) and
[validation status](docs/validation.md). The M3U/HLS milestone is complete in
[0.1.4](https://github.com/nilleiz/catchuparr/releases/tag/v0.1.4); XC player
validation remains open.
