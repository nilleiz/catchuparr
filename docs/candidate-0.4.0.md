# 0.4.0 validation and behavior

The 0.4.0 release adds Catchuparr archive playback to native Timeshift Stats and
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
The 0.4.0 release passed the local suite (400 tests), lint, compile,
package validation, independent review and full synthetic native integration
on both pinned versions. Native checks cover Stats routes and permissions,
Stop, timeouts and limits, recorder identity filtering, shared/private
isolation, source policies, failover and cross-process follower behavior.
Stats projection also passed under Dispatcharr's gevent runtime. Shared and
private recorder cases with `hide_recorders_in_stats` both enabled and disabled
were exercised, including private-recorder hiding enabled.

The WebSocket evidence uses the native event emitter and native consumer
authorization helper; it does not include a browser or network roundtrip, so
end-to-end WebSocket delivery is not established. A container crash observed
during diagnostics was not attributed to Catchuparr; no causal claim is made.
These server-side checks do not establish playback-client compatibility. The
release passed isolated Dev and user acceptance; the WebSocket check remains
limited to the native event emitter and authorization helper. See the
[validation record](validation.md) for the evidence and its limits.
