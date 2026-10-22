# Catchuparr 0.6.7 prerelease candidate

This candidate recovers a due, closed RETURN route when a different compatible
viewer connection is currently eligible. It verifies the exact closed episode
and cleanup, current recorder lease and applied generations, source catalog,
recording schedule and viewer identity before replacing the old intent with a
fresh single-use ATTACH intent.

During normal recorder reconciliation it also restores a missing Redis
sharing-authority projection from the exact committed outbox head. It refuses
recovery when an outbox transition is pending or its configuration, control,
catalog or revision does not match, and it does not replay old sharing episodes.
This lets recorders resume after Redis loses volatile keys without restarting
Dispatcharr or the container.

The normal live-stability and minimum-switch cooldowns remain in force. A
consumed RETURN claim, unchanged viewer identity, missing viewer, stale
authority or unconfirmed cleanup leaves the pending route untouched.

The candidate requires Dispatcharr 0.32.0 or newer. Focused synthetic tests
cover recovery, cleanup and deadline failures, lease loss, viewer absence,
single-use claims and the existing cooldown behavior. Required GitHub CI and
native acceptance remain pending. This document does not claim production
installation or real-player acceptance.
