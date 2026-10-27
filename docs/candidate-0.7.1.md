# Catchuparr 0.7.1 prerelease candidate

XC catch-up maps MPEG-TS presentation/decode timestamps and PCR clocks from
independent recorder attempts onto one archive-relative programme resource.
Encoded audio/video and byte positions are preserved. Arbitrary HTTP ranges
return the same bytes as the corresponding slice of a full response. Immutable
source clock metadata is cached with bounded memory; media is not duplicated
into an extra on-disk cache and playback does not start another provider feed.

The 0.7.0 behaviour stopped at the first gap/discontinuity, often leaving only a
short archive prefix. Normalized XC playback instead includes subsequent
playable segments with forward time jumps for missing recording time. It
excludes materially overlapping segments. Partial coverage is marked by
`X-Catchuparr-Archive-Partial: true` and `X-Catchuparr-Archive-Gaps: present`.
Mapped responses include `X-Catchuparr-Archive-Timeline: programme`.
Clients may not display these headers. External-player UI behaviour still needs
acceptance; codec/transport correctness alone does not establish that result.

Clock mapping supports aligned, unscrambled 188-byte MPEG-TS with readable
PES headers. Clockless or malformed material across unsafe boundaries is
rejected. The legacy explicit continuous raw mode keeps its existing contract.
Stream limits, channel/network checks, session cancellation and archive leases
remain active. Missing media is not reconstructed or replaced with live TV.

Native HLS requests for a programme still recording remain appendable inside
the selected programme bounds. Completed historical requests remain closed;
later programmes are excluded. Fresh recorder muxer attempts explicitly mark
an MPEG timestamp discontinuity even when their wall times are adjacent.

## Verification and remaining work

Focused regressions use synthetic audio/video from independent muxer runs and
check complete decoder output, a programme timeline spanning a gap, timestamp
wraparound, HTTP byte ranges and growing bounded HLS manifests. Existing raw TS
safety checks remain required. The authoritative Dispatcharr 0.32.0/0.33.0 native
matrix must accept the final source tree before publication.

## Borrowed-connection stability

The native observer previously read the buffer index before the newest chunk.
A writer could publish a newer chunk between these reads, making a healthy
connection appear inconsistent and ending its borrowed recorder. A deterministic
interleaving regression fails on the old code and passes when the timestamp set
is sampled before its upper index bound. Both tested native writers publish the
index before that set. Owner, source/profile, lease, client and missing-chunk
checks remain active, with fixed rejection categories exposed in borrow errors.

This prevents the reproduced false invalidation and its fallback/re-attach
cycle. It does not reconstruct missing recordings or claim that every historic
failure had this cause: older logs retain a generic observation rejection
without its individual failed predicate. Live acceptance requires an existing
ordinary viewer connection; no provider session is started by diagnostics.

### Delayed borrowed-client cleanup

A borrowed HTTP stream may finish its native close callback after FFmpeg has
already exited. The recorder now retains its current lease while retrying the
exact cleanup acknowledgement, rather than exiting on the first pending result.
It starts no replacement input before durable episode closure and exact private
client absence are confirmed. Stop, changed admission and lost lease interrupt
this wait. Diagnostics do not decide resource ownership.
