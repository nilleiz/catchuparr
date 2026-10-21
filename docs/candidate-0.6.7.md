# Catchuparr 0.6.7 prerelease candidate

This candidate recovers a due, closed RETURN route when a different compatible
viewer connection is currently eligible. It verifies the exact closed episode
and cleanup, current recorder lease and applied generations, source catalog,
recording schedule and viewer identity before replacing the old intent with a
fresh single-use ATTACH intent.

The normal live-stability and minimum-switch cooldowns remain in force. A
consumed RETURN claim, unchanged viewer identity, missing viewer, stale
authority or unconfirmed cleanup leaves the pending route untouched.

The candidate requires Dispatcharr 0.32.0 or newer. Focused synthetic tests
cover recovery, cleanup and deadline failures, lease loss, viewer absence,
single-use claims and the existing cooldown behavior. Required GitHub CI and
native acceptance remain pending. This document does not claim production
installation or real-player acceptance.
