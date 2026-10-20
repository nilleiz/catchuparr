# Catchuparr 0.6.4 prerelease candidate

This candidate makes completion of a pending return to an allowed source
recoverable and idempotent. The recorder uses the timestamp of its first
committed media; the supervisor verifies the current lease, applied sharing
authority, source candidate, and completed borrowed-session cleanup before
committing the state transition. A retry can recognize that exact transition
if the database committed but its acknowledgement was lost.

The supervisor performs this completion before its next viewer observation,
so a newly eligible viewer can be attached in the same supervision pass.
Concurrent media-callback and supervisor acknowledgements are safe for the
same recorder attempt. Stale media, a changed lease or applied source, and an
unconfirmed native-client cleanup remain fail-closed.

The candidate requires Dispatcharr 0.32.0 or newer. Focused unit tests and
native acceptance are pending; this document does not claim playback-client
acceptance.
