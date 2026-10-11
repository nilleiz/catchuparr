# 0.4.0 candidate

This candidate adds Catchuparr archive playback to native Timeshift Stats and
adds an independent control for hiding positively identified Catchuparr
recorders from live Stats.

Both options default to enabled and are saved with the existing unified Apply.
Saving an editable draft does not change Stats behavior. Existing valid 0.3.1
settings snapshots remain readable; their missing options use the enabled
defaults until the next Apply.

Archive viewers use opaque display IDs. These IDs are not playback credentials
and do not reveal token URLs. A viewer stays associated with the same logical
user, channel and playback identity across seeks and session replacement. A
successful playback heartbeat refreshes the Stats row; the row expires after
180 seconds without one. Position, pause state and stream details remain
unknown where playback does not provide an observed value.

Recorder hiding uses short-lived server capabilities tied to the active fenced
recorder lease. It preserves ordinary viewers on shared channels and adjusts
only the returned Stats copy. It does not change provider reservations, native
client limits or live stream state. The native Stop action accepts Catchuparr's
opaque display ID only after native admin authorization and revokes its local
playback leases.

The projection hooks are signature checked for Dispatcharr 0.31.0 and 0.32.0.
The 0.4.0 candidate passed the local suite (392 tests), lint, compile, package
validation, independent review and full synthetic native integration on both
pinned versions. Native checks cover Stats routes and permissions, Stop,
timeouts and limits, recorder identity filtering, shared/private isolation,
source policies, failover and cross-process follower behavior.

The WebSocket evidence uses the native event emitter and native consumer
authorization helper; it does not include a browser or network roundtrip.
Shared-recorder `hide_recorders_in_stats=True` and private-recorder
`hide_recorders_in_stats=False` were exercised. Private-recorder output with
`hide_recorders_in_stats=True` remains unverified. Required CI, isolated Dev
installation and real-player acceptance remain pending. These checks do not
establish playback-client compatibility or indicate a published release.
