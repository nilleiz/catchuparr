# Catchuparr 0.7.2 prerelease candidate

The recorder watches native viewer/source changes and admitted switch deadlines
more frequently, and a switch signal wakes its current FFmpeg attempt promptly.
An exact pending cleanup is retried more frequently. Routine viewer stability,
last-viewer grace and switch cooldown settings remain in force.

After a borrowed attempt fails, the recorder first confirms the exact native
cleanup and rechecks current lease, configuration, control, catalog and viewer
authority. It may then create one fresh, single-use attach claim without waiting
for normal viewer-driven cooldowns. No consumed claim is replayed. If no viewer
is eligible, the existing allowed-source fallback remains available. Each
native proof rejection now uses a fixed reason category without sensitive
identifiers. Older generic errors do not establish their exact cause.

HLS segment responses and normalized XC responses map MPEG-TS clocks for
independent muxer sessions onto a continuous available-media timeline. Missing
archive intervals are skipped instead of being replaced with live content or
held open as a long timestamp jump. A small decoder guard separates independent
sessions. HLS programme-date tags retain archive wall-clock times. The XC
progress duration is consequently shorter than wall-clock programme duration
when recording time is missing. The files on disk, encoded media payloads and
HTTP byte-range semantics are preserved. Clockless material retains existing
safe fallback or rejection behaviour; this is not a universal codec guarantee.

An optional setting, disabled by default, briefly stages the existing viewer
input while a dedicated recorder continues. It attempts a content and clock
match across a complete audio/video segment, freezes publication at the proven
boundary and continues that same staged reader. For a normal return, it first
confirms exact borrowed-client cleanup and then compares the new dedicated
reader with the archived tail. Either direction continues the same reader
without discarding eligible media if proof is unavailable. Failed borrows use
fast recovery without waiting for this comparison. This does not guarantee
gapless archives; no transition can recover data that neither input supplied.

Focused regressions use synthetic video and audio from independent muxers,
including changing video dimensions, adjacent and missing archive intervals,
unaligned byte ranges and an unchanged-muxer sequence. Real-player acceptance
is separate. The final tree must pass the authoritative Dispatcharr 0.33.0
native and package jobs before this candidate can be published.
