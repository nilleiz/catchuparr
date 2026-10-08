# Repository Guidelines

## Dev stack and restore policy

For Dev plugin updates, back up the isolated application data and configuration
only. Exclude the archive, recordings and archive database unless the user
explicitly requests an archive backup. Record that exclusion in rollback notes.
Keep at most two verified Dev backups. Verify a new backup before deleting older
task-owned Dev backups; failed or partial backups do not count as restore points.
Never apply this retention policy to production or unrelated backups.

Match the production Dispatcharr topology: run one Dispatcharr AIO container
with its embedded PostgreSQL, Redis and workers. Bind only isolated Dev data,
archive and port paths. For the next Dev rebuild, use an existing Dispatcharr
backup ZIP from `containerfiles` and Dispatcharr's authenticated backup restore
API as the default restore path. Copy the ZIP into the isolated Dev backup
directory; never mount a production directory writable into Dev. Pause Dev
Celery Beat before restoring, keep Dev egress blocked, apply
`deploy/dev/scrub.sql` before resuming Beat, and verify all imported providers,
plugins and periodic jobs remain disabled. Use a raw PostgreSQL restore only
when no compatible Dispatcharr backup ZIP is available. The verified procedure
and API routes are in `docs/development.md`.

## Project Structure & Module Organization

Plugin code lives in `catchuparr/`, tests in `tests/`, package tooling in
`scripts/`, deployment files in `deploy/dev/`, and documentation in `docs/`.

## Build, Test, and Development Commands

Run `python3 -m unittest discover -s tests -q`,
`python3 -m compileall -q catchuparr scripts tests`, `ruff check .`, and
`python3 scripts/build_plugin.py`. The Dev stack is started with
`docker compose --env-file <private-dev-env> -f deploy/dev/compose.yml up -d`.

## Coding Style & Naming Conventions

Follow the formatter and linter selected for the project’s language once configured; avoid introducing a second style tool. Use descriptive names, keep modules focused, and match existing patterns. Name test files after their subject (for example, `parser.test.*`) and use kebab-case for static asset filenames unless the chosen framework requires another convention.

## Testing Guidelines

Add focused `unittest` tests under `tests/` alongside behavior changes.
Synthetic streams are preferred for archive and playback tests.

## Commit & Pull Request Guidelines

Use short imperative commit subjects. Feature changes go through small PRs with
test results and a clear user impact. No upstream PR without user approval.

## Security & Configuration

Keep credentials and environment-specific values out of source control. Use documented local environment variables or ignored configuration files, and never commit secrets from `.aws/` or other local tooling directories.

## Privacy in repository content and publication

Describe the plugin's behavior, requirements and verified compatibility using
generic, reproducible examples. Keep descriptions of the user's personal setup
and real-world test environment out of repository content. This rule applies
even when the repository is private and even when the information has appeared
in this chat or earlier commits. Prior disclosure is not permission to repeat it.

Scope includes the GitHub repository description, README, documentation,
roadmap, examples, test fixtures, code comments, commit messages, PRs, issues,
release notes, package contents, screenshots and CI logs/artifacts.

Never include:

- Real channel names, programme names, programme descriptions, personal EPG
  entries or other information about the user's viewing or recording history.
  Use invented fixtures such as `Synthetic Channel A` and `Synthetic Programme`.
- The user's playback device, device model, installed playback apps or their
  versions, or any attribution linking compatibility results to the user.
  General product compatibility may be documented without personal test details.
- Personal hostnames, usernames, home-directory paths, local IP addresses,
  network topology, ports, container/stack names, volume names, archive paths,
  provider account identifiers or tuner allocation from the user's setup.
  Use placeholders and synthetic local examples instead.
- Credentials, tokens, cookies, authenticated URLs, usable session identifiers,
  configuration exports, backups, databases, recordings, EPG exports or raw
  request/response payloads from a personal or production environment.
- Screenshots, traces or attachments that reveal any of the above, including
  indirect identifiers, embedded metadata and exact real-world test timestamps.

Private operational notes needed to deploy or diagnose belong in ignored local
files outside tracked documentation and package inputs. Report public test
evidence as the tested behavior, software compatibility where relevant, outcome
and limitations; omit personal examples and setup details. Sanitization must
remove identifying values rather than merely masking credentials in a URL.

### Review, merge and release blockers

- Block commits, PR publication, merges and releases if their content or generated
  artifacts contain the prohibited information. Review the diff, descriptions,
  generated package contents and intended CI artifacts before publication.
- Existing disclosures in files included in a change or release are blockers
  too. Replace them with neutral examples before publishing the affected content;
  a clean new diff alone is insufficient.
- Block publication of unsanitized diagnostic output, copied chat excerpts,
  real EPG/recording fixtures or screenshots. Use synthetic data for CI and
  share only explicitly sanitized diagnostic summaries.
- Block claims of test success or compatibility without verified evidence.
  Distinguish synthetic tests, integration checks and real-player acceptance
  without identifying the user's player or viewing history.
- Block a change that removes useful technical evidence instead of providing a
  privacy-preserving equivalent, or that weakens required checks to obtain a
  passing result. Preserve reproducible steps, results and known limitations.
- If credentials or private data were already published, stop further exposure,
  report the affected surface without repeating the data, and prepare remediation.
  Do not rewrite shared Git history or delete published assets without explicit
  authorization; ordinary removal in a new commit does not erase earlier exposure.

Treat uncertain personal details as private and use a neutral placeholder.
Permission to implement, deploy or publish a release does not authorize disclosure
of personal setup or test information.
