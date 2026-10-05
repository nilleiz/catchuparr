# Repository Guidelines

## Dev stack and restore policy

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
