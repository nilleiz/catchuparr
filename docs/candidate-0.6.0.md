# Catchuparr 0.6.0 prerelease candidate

Version 0.6.0 adds optional reuse of an already-running, compatible native
Dispatcharr live connection while an ordinary viewer is using it. The plugin
reads the existing worker's published MPEG-TS buffer and owns a separate
reader client. It does not initialize an upstream channel, reserve another
provider slot, or create a source assignment for borrowing. Sharing is off by
default and only applies to channels selected by the applied channel filter.

This is a candidate record. Required CI and complete end-to-end acceptance on
the pinned Dispatcharr 0.32.0 release are not yet approved. The candidate has not
been published. Do not infer native runtime acceptance from local unit tests.

## Enable and configure

In Dispatcharr, open the Catchuparr plugin settings and enter a version 1 YAML
document in **Recorder connection sharing (YAML)**. Apply it with the channel
filter and the rest of the plugin settings.

```yaml
version: 1
default:
  enabled: true
  live_stability_seconds: 5
  last_viewer_return_seconds: 30
  min_switch_interval_seconds: 60
rules:
  - channels:
      numbers: [100]
    min_switch_interval_seconds: 90
  - channels:
      names: ["Synthetic Channel B"]
    enabled: false
```

Blank sharing YAML disables the feature. Enabling `default.enabled` makes it
eligible on all channels selected by the channel filter; selector rules can
override the defaults. The three cooldown values are integers from 0 through
3600. Booleans, decimals and numeric strings are invalid. Zero disables only
the corresponding cooldown. Per-channel settings are persisted through Apply.

Sharing borrows only a healthy connection with compatible source assignment,
account, profile, input and native owner state. A currently viewed channel may
be borrowed even when that source is outside Catchuparr's allowed source list,
because no new upstream connection is opened. If there is no compatible
viewer connection, the recorder can use only its allowed assigned sources; it
does not make assignments. If no source is available, recording waits paused.

## Timing and cleanup

- A live connection must remain stable for the configured live-stability time
  before a borrow can begin. Ordinary viewer joins and leaves do not reset the
  connection's stability clock.
- After the last ordinary viewer leaves, the recorder waits for the configured
  return grace and any remaining minimum switch interval before returning to
  an allowed source.
- A successful switch ends the current FFmpeg attempt and begins a new one
  with a segment discontinuity. The recorder keeps its channel fence.
- Explicit Stop, authority invalidation and recorder failure use prompt
  cleanup; they do not wait for ordinary viewer cooldowns.
- Cleanup must acknowledge the exact borrowed client and episode before the
  recorder launches another source. Stop does not stop the ordinary viewer's
  native channel.

## Release acceptance still required

Before publication, run the required CI workflow and the pinned 0.32.0 AIO probe.
The native probe must cover borrowing a source outside the allowlist without a
new provider request, retaining unrelated viewers through Stop, exact cleanup
at the first fallback provider request, first-viewer reuse, grace and switch
deadlines, repeated borrow/return cycles, Apply and Stop revocation, restart
recovery, stale capabilities, failure and acknowledgement retry. Verify that
the tested source tree and ZIP are identical and that the archive contains the
Dispatcharr icon. See the [validation record](validation.md) for current
evidence and pending gates.

The plugin icon is the existing 256-pixel repository asset. Dispatcharr loads
the optional `logo.png` placed beside the plugin entry point; the release ZIP
includes that file at `catchuparr/logo.png`.
