# Catchuparr 0.6.1 prerelease candidate

Version 0.6.1 keeps the 0.6.0 native sharing behavior and replaces per-channel
sharing YAML with four global controls: an enable switch, live stability delay,
last-viewer return delay and minimum interval between later switches. Sharing is
off by default and applies only to channels selected by the applied source
filter. A first attach to an already eligible viewer is immediate; later
switches retain the configured timing and current security checks.

The 0.6.0 native acceptance passed on the pinned Dispatcharr 0.32.0 AIO. The
0.6.1 configuration migration and initial-attach behavior have focused unit
coverage. Required GitHub CI and publication remain pending; real-player
acceptance is not established.

## Configure

In Dispatcharr plugin settings, enable **Share existing live connections** and
set each delay to an integer from 0 through 3600 seconds. Zero disables that
delay. Apply these values together with the channel filter and other settings.

Uniform values in an older applied configuration migrate automatically,
including the enabled state and all three delays. Conflicting per-channel
values require an explicit choice: select the desired global values and enable
**Confirm global sharing settings migration**. Until then, Validate and Apply
fail without changing the active snapshot. New active snapshots store only the
global sharing settings; runtime expands them across selected channels.

## Sharing behavior

Catchuparr reuses only a healthy compatible native connection that already has
an ordinary viewer. It does not initialize another upstream stream or use an
excluded source to start a connection. After the last viewer leaves, the
recorder waits for both the configured return grace and any remaining minimum
switch interval. Explicit Stop, authority invalidation and recorder failure
use prompt cleanup.

The complete 0.6.0 native acceptance checklist and evidence are recorded in
the [validation history](validation.md). Required GitHub CI and publication
remain release gates.
