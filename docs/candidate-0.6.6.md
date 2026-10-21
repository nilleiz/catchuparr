# Catchuparr 0.6.6 prerelease candidate

This candidate resumes the same admitted ATTACH intent when a transient
pre-claim validation miss occurs and its durable single-use claim is still
unused. Retry rechecks the current recorder lease, applied configuration,
control generation, source catalog and exact eligible viewer identity. The
existing SQLite claim remains the single-use boundary; a consumed claim is not
replayed.

The candidate requires Dispatcharr 0.32.0 or newer. Focused synthetic tests
cover retry, stale authority, consumed claims and concurrent polls. Required
GitHub CI and native acceptance remain pending. This document does not claim
playback-client acceptance.
