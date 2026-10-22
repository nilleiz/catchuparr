# Catchuparr 0.6.8 prerelease candidate

A failed source can finish stopping before its native cleanup acknowledgment
arrives. Previously the recorder ended its task on the first missing ACK.
Reconciliation then restarted at the highest-ranked source, which could repeat
the same failure indefinitely despite another permitted source being available.

The recorder now keeps its lease and position in the allowed source list while
retrying that exact attempt's revocation, native stop and final acknowledgment.
It opens the next permitted source only after cleanup is confirmed. The
supervisor keeps checking control, configuration and ownership during cleanup
without renewing the deliberately revoked capability. Pause, stale admission or
lease loss ends the wait safely. No additional input opens on uncertain cleanup.
The same recovery applies to dedicated sources with sharing enabled or disabled.

Focused regressions reproduce delayed native stop, delayed HTTP acknowledgment,
transient cleanup failure, cancellation and lease loss. The existing native
Dispatcharr 0.32.0 failover probe now withholds a real successful cleanup ACK,
checks that the same lease stays active and no replacement source opens, then
requires useful archived media from the next allowed source after ACK release.

This candidate requires Dispatcharr 0.32.0 or newer. Required GitHub CI and native
acceptance are pending. Provider failures can still occur; this change allows
the recorder to recover without repeatedly restarting at the failed source.
