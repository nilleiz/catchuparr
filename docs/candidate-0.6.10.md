# Catchuparr 0.6.10 prerelease candidate

Dispatcharr 0.33.0 moved channel authorization out of the private timeshift
helper into its central channel-access module. Catchuparr now delegates to that
shared policy, retaining its adult-content and profile-visibility rules. The
legacy helper is used only when the shared module is absent; denial, import
failure inside the shared implementation, or execution failure stays denied.

The minimum admitted Dispatcharr version remains 0.32.0, with runtime capability
checks in addition to version admission. Native CI now runs the complete probe
against pinned 0.32.0 and 0.33.0 images. Release reuse requires both native jobs,
their actually executed successful playback steps, and the package job from the
exact source tree. No additional release AIO is required for the identical tree.

Focused regressions cover both policy paths, fail-closed behavior, and missing,
failed or skipped native jobs. The native probe preserves each core's strict
credential and channel-denial contract and exercises the shared adult-content
policy. Authoritative native acceptance is pending. No production upgrade is
part of this change.
