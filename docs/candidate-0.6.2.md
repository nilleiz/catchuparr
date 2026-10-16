# Catchuparr 0.6.2 prerelease candidate

Version 0.6.2 lets authenticated M3U/XMLTV HLS and XC playback continue across
programme boundaries without requiring each later EPG row. Playback remains
bounded: M3U/XMLTV sessions cover at most 24 hours from the requested start;
XC follows contiguous indexed transport-stream segments within Dispatcharr's
duration limit. An XC stream stops at an indexed gap and waits no longer than
125 seconds for a next segment at the live edge. The bound covers the
recorder's 120-second media-idle timeout and five seconds for indexing and
finalization; a missing recorder lease closes follow mode promptly. HLS uses
the same 125-second edge window and recorder lease, with its 24-hour session
bound.

Focused synthetic tests cover multiple programme boundaries, appending new
segments during playback, seek behavior, missing EPG metadata, cleanup on
client disconnect, and finite behavior for gaps and stopped recorders. Required
GitHub CI and real-player acceptance remain pending. No production installation
is authorized by this candidate note.

The 0.6.1 global connection-sharing configuration and migration behavior are
retained. Dispatcharr 0.32.0 remains the minimum supported version, and the
existing plugin icon remains in the package.
