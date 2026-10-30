# Catchuparr 0.7.4 prerelease candidate

When a recorder switches to a viewer's stream, or returns to a dedicated
source, the new input normally begins several seconds before the end of the
archive. Earlier versions archived that overlap twice or, when the previous
input ended first, could leave a gap.

0.7.4 finds the last audio and video packets of the archive in the new input by
packet hash, requires a unique match and a keyframe at the resume point, and
cuts the new segment on whole MPEG-TS packets. No media is re-encoded or
re-timed. A staged input adopted without a prior proof is trimmed the same way,
and staged segments that only repeat archived media are discarded. Without a
provable overlap the input is used unchanged as a marked discontinuity.

Audio frames share PES packets and cannot be split without rewriting them, so
audio resumes at the next PES start. A fraction of a second of audio may be
omitted; none is repeated. One video track and one or more audio tracks are supported; other layouts keep
the previous behaviour.

"Try to synchronize before switching streams" is now enabled by default for new
configurations. Existing applied configurations keep their stored value.

Focused regressions use synthetic MPEG-2/MP2 and H.264 (B-frames)/AAC streams
and two live readers with different segment boundaries. Real-player acceptance
is separate. The final tree must pass the authoritative native and package jobs
before publication.
