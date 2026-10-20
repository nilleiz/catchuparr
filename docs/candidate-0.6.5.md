# Catchuparr 0.6.5 prerelease candidate

This candidate preserves the persisted RETURN admission deadline through
first-media completion. A RETURN admitted after recorder failure can complete
on its first useful media even when the ordinary viewer-return or minimum
switch cooldown has not elapsed. Ordinary RETURN transitions continue to wait
until their persisted deadline.

Completion remains bound to the current RETURN token, recorder lease, applied
configuration and control generations, source candidate, sharing authority,
native cleanup acknowledgement, recorder fence and intent epoch. The exact
first-media timestamp is retained across retries, and SQL completion remains
an exact one-revision compare-and-swap.

The candidate requires Dispatcharr 0.32.0 or newer. Focused ledger tests pass;
required GitHub CI and native acceptance remain pending. This document does
not claim playback-client acceptance.
