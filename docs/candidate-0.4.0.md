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
Synthetic tests cover projection copies, independent option combinations,
websocket refresh, recorder identity, session Stop and playback restart. Native
application checks against both pinned versions and real-player acceptance are
release gates and are not represented by these synthetic tests.
