# CI and release evidence

Pull requests run the unit, package, and pinned Dispatcharr 0.32.0 AIO checks.
The AIO job has a 40-minute outer timeout for its 30-minute probe budget plus
bounded setup and cleanup time; the probe's own deadline and assertions remain
the source of its runtime limit.
The CI workflow can also be dispatched manually for an explicit recovery or
diagnostic run. Pull-request runs use concurrency cancellation so an obsolete
PR revision does not continue consuming an AIO runner. CI does not rerun on
every push to `main`.

The `test-and-package` job uploads the plugin ZIP with a provenance document.
That document records the repository, CI workflow ref, run and attempt IDs,
source commit and Git tree, package file inventory, and ZIP SHA-256. It contains
no credentials or runtime configuration.

The release workflow does not invoke CI. It searches successful, same-repository
pull-request runs of the trusted CI workflow and accepts a run only when its
Git tree exactly matches the release checkout. A merge commit SHA can differ
from the release commit SHA; their tree identities must match. The
`test-and-package` and `AIO integration (0.32.0)` jobs must both have succeeded.

Before release publication, the workflow downloads that run's CI artifact and
checks its run identity and attempt. For a pull-request run, the artifact's
tested synthetic merge commit must have exactly the base and head commits
recorded for that run as its parents. The workflow then compares that tested
commit's tree with the release checkout. This also permits a squash merge when
the resulting release tree is identical. The workflow checks the artifact
checksum, package file inventory, and every packaged file byte against the
release checkout. Existing version, changelog, main-branch, tag, and release
checks remain in force.

Missing, expired, failed, mismatched, or incomplete evidence stops publication
with an instruction to rerun the complete pull-request CI workflow explicitly
for the exact source tree. A manual CI dispatch is useful for diagnosis, but
the release accepts pull-request CI provenance only. The release workflow never
starts another AIO run automatically.
