# Development and deployment

## Local checks

Run `python3 -m unittest discover -s tests -v`, `python3 -m compileall -q catchuparr scripts tests`, and `ruff check .`. Build the importable archive with `python3 scripts/build_plugin.py`.

## Installing the plugin

Import `dist/catchuparr-<version>.zip` from Dispatcharr's Plugins page and enable it after inspecting its settings. The plugin code is installed under Dispatcharr's `/data/plugins/catchuparr`. The AIO container runs web and Celery with the same `/data/catchuparr` archive mount. During an update, stop the recorders, install the new ZIP, reload plugins, run the compatibility check, and resume the selected channels. Keep the prior ZIP and a verified application/configuration restore point until playback smoke tests pass; use the backup policy below.

For the unreleased 0.3.0 candidate, use the
[candidate guide](candidate-0.3.0.md) for YAML schedules, recorder controls,
legacy-setting reset behavior and current acceptance status. The build command
uses the manifest version when naming the ZIP.

Dispatcharr v0.31.0 and v0.32.0 accept a manual update at authenticated admin
`POST /api/plugins/plugins/import/` with multipart field `file` and explicit
`overwrite=true`; the response must contain `success: true` and the expected
plugin key/version. `POST /api/plugins/plugins/reload/` refreshes web-process
discovery. Restart the isolated AIO after the import so its Celery workers use
the same package, then verify plugin status, recording and playback. Keep the
previous ZIP for rollback. Both POST routes require an admin account in the
inspected image.

The inspected compatibility targets are Dispatcharr v0.31.0 and v0.32.0. Unknown versions are refused. Custom images must retain the inspected core signatures and pass request tests before enabling XC output.

Create a separate archive access token for each playback device. Session identity is scoped to the credential, not proof of physical device identity: copying a playlist to another player shares its slot. Playlist reloads for the same programme reuse its session; switching programmes replaces that credential's session while preserving its previous segment URLs for a 30-second grace period.

The authenticated M3U/XMLTV endpoint provides HLS archive playback. Its
start-over, forward/backward seeks and pause/resume passed real-player
acceptance with Dispatcharr 0.32.0. The plugin endpoint uses `{utc}`
epoch seconds and `{duration}` seconds;
Dispatcharr's native XC timeshift endpoint uses `{duration:60}` minutes. Keep
those contracts separate. UTC metadata is attached only to locally annotated
entries, so provider catch-up metadata is preserved. XC TS/Range playback must
pass the AIO integration checks and separate real-player acceptance before being
described as player-validated. Local availability is never persisted into
Dispatcharr's provider-derived channel catch-up fields.

On each M3U seek, the endpoint resolves the requested UTC instant to the
effective Dispatcharr guide programme. For older entries removed by an EPG
refresh, it uses a locally saved programme snapshot only when that entire
historical programme has archive coverage, matching the plugin XMLTV export.
An unknown programme returns 404. The initial HLS playlist stops at that
programme's end even if a client sends its original full duration after a
seek. HLS reloads may append newly indexed segments within that programme.
Fetching or prefetching its tail cannot unlock the next programme. Observed
client-managed continuation requested a new archive window at a programme
boundary.
Server-side extension of the old playlist stays disabled; other clients need
their own continuation test. A segment crossing the programme's end is omitted
rather than serving
content from the next programme; playback may therefore end one segment early.
The playlist target stays at 60 seconds because stream-copy segmentation
can have delayed keyframes.

Local XC windows use the exact positive duration hint in minutes. With no
usable hint, the actual EPG programme end determines the remaining window;
without reliable EPG metadata, playback delegates to the provider. Dispatcharr's
extra provider duration padding is never required for local archive coverage.
Use the plugin XMLTV endpoint with the plugin M3U export. Native XC M3U tags
do not extend the core XMLTV endpoint's historical guide.

## Backup and rollback scope

By default, Dev restore points cover application data and configuration only.
Exclude the archive directory, recordings and archive database. Keep at most two
verified task-owned Dev backups, and prune an older one only after the new
restore point has been verified. Include archive data only with explicit
authorization. An application/configuration-only restore point cannot recover
archive files or database content that was lost or overwritten.

Rollback uses the previous plugin/image and its matching application/configuration
restore point while preserving the current archive directory, recordings and
archive database. Do not restore or replace archive data as part of an ordinary
rollback.

## Upgrading the isolated AIO to 0.32.0

Use `ghcr.io/dispatcharr/dispatcharr@sha256:b7d695c5cc98b9abd64c74a94539c1ebffc23d965021613c820c67e25dea41a3`
with `DISPATCHARR_ENV=aio`. This remains one container with PostgreSQL 17,
Redis, web and workers. The official image replaces the Dev custom image only;
production is not updated. Preserve Dev bind addresses, volumes and egress rules.

1. Stop only the Dev AIO and verify it is stopped. Create and verify a private
   restore point for application data and configuration only. Exclude the
   `archive` directory, recordings and archive database. Retain the previous
   plugin ZIP and private image digest. Keep no more than two verified task-owned
   Dev restore points.
2. Verify `scripts/dev_egress.py check` against the agreed Dev subnet and source.
   Change only the private `DISPATCHARR_DEV_AIO_IMAGE` pin, then recreate the Dev AIO.
3. Verify Dispatcharr version, migration completion and PostgreSQL major version.
   Confirm only the intended test provider, Catchuparr and its schedules are active.
   Import the updated plugin through the authenticated API and restart Dev AIO
   so web and workers load the same version. Check anonymous requests are denied,
   recording progresses and both output adapters obey user catch-up restrictions.
4. Repeat real-player tests for start-over, pause, multiple seeks,
   programme boundary, restart and retention. Enable sanitized tracing only for
   the test and record numeric time/range/status fields; never capture full URLs.
5. To roll back, stop Dev and retain diagnostic data privately. Reinstall the
   previous plugin/image and restore its matching application/configuration
   restore point, then recreate Dev. Preserve the current archive directory,
   recordings and archive database; the application/configuration restore point
   cannot recover archive data if it was lost. Do not run the old application
   against a database migrated by the new release.

For a rebuild instead of an in-place upgrade, use the backup ZIP/API restoration
procedure below. Run `python3 scripts/run_aio_integration.py --image <pinned-image>`
for a disposable, networkless AIO probe; it refuses databases containing users or
channels. CI runs the same synthetic probe against both supported releases and
collects no container data, recordings or credentials as artifacts.

## Isolated development stack

Never mount production directories into Dev. Keep host-specific names, addresses and source backup paths in private deployment records outside this repository.

1. Inspect production read-only and pin its **AIO** image digest. Prefer an existing Dispatcharr backup ZIP under the private `containerfiles` tree. Copy it into a private Dev staging directory, check its SHA-256 and ZIP integrity, and verify `metadata.json` says `format=dispatcharr-backup`, `version=2`, `database_type=postgresql`. The ZIP contains the database; copy required logos, M3U and EPG files separately. Never copy recordings, a live PostgreSQL data directory, JWT material or credentials into source control.
2. Set `DEV_AIO_ROOT`, `DEV_BIND_IP` and `DISPATCHARR_DEV_AIO_IMAGE` in a private environment file. Create an empty `${DEV_AIO_ROOT}/data` and `${DEV_AIO_ROOT}/archive` owned by UID/GID 1000. Copy the ZIP into `${DEV_AIO_ROOT}/data/backups` **as a private copy**, without bind-mounting the production backup directory. Create a dedicated bridge with an unused private `DEV_LAN_SUBNET`, for example `sudo docker network create --driver bridge --subnet "$DEV_LAN_SUBNET" catchuparr-dev-lan`. Run `sudo python3 scripts/dev_egress.py apply --subnet "$DEV_LAN_SUBNET"` and then `check`. This initially denies all new Dev egress while allowing established replies. Do not start Dev if `check` fails.
3. Start `deploy/dev/compose.yml` as `catchuparr-dev`. It runs the same AIO image as production: embedded PostgreSQL, Redis, web and Celery in **one container**. `catchuparr-dev-lan` is external, so Compose cannot create an unrestricted replacement. Confirm that port 15656 binds only to the chosen Dev address and the container has only Dev mounts. Create the initial Dev admin account through the local UI. Before restoring, pause **only the Dev Celery Beat process** with `SIGSTOP`; keep the Celery worker running to execute the API restore. Record the Beat PID and verify it is stopped. The Dev egress block remains active throughout.
4. Restore using an authenticated **Dev admin** request: `POST /api/backups/<copied-zip-filename>/restore/`. In Dispatcharr v0.31.0, this route requires `IsAdmin`, returns HTTP 202 with `task_id` and `task_token`, and runs the restore in Celery. The backup must already be in Dev `/data/backups`; `POST /api/backups/upload/` is an alternative to copying it. Check completion at `GET /api/backups/status/<task_id>/?token=<task_token>` or through the Dev admin UI. Keep the token private; the restore can invalidate the initial admin session. A completed API task is still followed by a database content check. Do not assume elapsed time means success.
5. While Beat remains stopped, apply `deploy/dev/scrub.sql` to **Dev** PostgreSQL (`docker exec -i catchuparr-dev-aio psql -U dispatch -d dispatcharr -v ON_ERROR_STOP=1 < deploy/dev/scrub.sql`). Verify zero enabled periodic tasks, provider accounts, EPG sources and plugins. The scrub disables DVR rules, recording jobs, provider refreshes, notifications and integrations too. Then send `SIGCONT` to the recorded Beat PID and restart only the Dev AIO container. Recheck the zero counts and an unauthenticated-denial endpoint. Select one test channel only after agreeing on its source connection and tuner usage.
6. Before enabling a recorder, agree on a numeric `DEV_VU_IP` and `DEV_VU_PORT`. Stop Dev AIO; run `sudo python3 scripts/dev_egress.py remove --subnet "$DEV_LAN_SUBNET"`, then `apply` and `check` with `--vu-ip "$DEV_VU_IP" --vu-port "$DEV_VU_PORT"`; restart Dev. The rule permits established replies and new TCP connections only to that Vu+ endpoint. If the endpoint redirects elsewhere, leave it blocked and revise the allowlist explicitly.
7. For a real player test, bind `DEV_BIND_IP` to the Dev host's LAN address and recreate only the Dev AIO. A fixed player address is optional; the M3U, XMLTV and archive routes authenticate with a separate, revocable token for that test device. Keep URLs containing the token in a private file outside the repository. Verify that an unauthenticated LAN request is rejected before importing the M3U and XMLTV URLs in the test player. Keep player/device details in private operational notes. Test XC and M3U/XMLTV separately: archive icon, live start-over, repeated seeks, pause/resume, programme boundary, service restart, retention and rollback. Record HTTP method, URL template, time arguments, Range headers and response codes without logging credentials.

For a Dev player trace, set `CATCHUPARR_TRACE_REQUESTS=1` in the private Dev
Compose environment. AIO 0.32.0 starts web workers through `su -`, which strips
custom environment variables. For that image, create an empty private marker
`/data/plugins/catchuparr/.trace-requests` inside Dev after plugin installation;
this enables the same sanitized traces without changing Dispatcharr core files.
Remove the marker after the player test; imports may remove it, so recreate it
only when another diagnostic test is intended. The plugin then logs the
archive UTC start, duration, EPG end, first/last segment times and response
status, plus each segment's method, numeric Range and status. It does not log
bearer tokens or leases in these trace lines. Collect only lines beginning
`Catchuparr archive` or `Catchuparr request`, keep
them private, and set the flag back to `0` after the test. Do not publish raw
web-server access logs because URL query strings may contain bearer tokens.

Release assets include the installable ZIP and `SHA256SUMS`. Download both
from the tagged GitHub release and run `sha256sum -c SHA256SUMS` in the download
directory before importing. Retain the previous package and a verified
application/configuration-only Dev restore point for rollback, following the
two-backup limit above. Archives, recordings and the archive database are
excluded unless explicitly authorized. A release does not authorize a
production deployment.

Use the verified Dispatcharr backup ZIP and API flow above by default. If no compatible ZIP exists, use `pg_dump -Fc`, verify it with `pg_restore -l` in a networkless container, restore into an isolated temporary PostgreSQL 17 container, apply `scrub.sql`, stop it, and copy only its cold cluster into the AIO Dev `/data/db` path (UID/GID 1000). A cold cluster prepared under a different glibc version may need `REINDEX DATABASE` for each copied database, followed by `ALTER DATABASE ... REFRESH COLLATION VERSION` inside the AIO image. Never mount a production database directory into Dev.

After stopping the Dev AIO container, remove the egress rule with `sudo python3 scripts/dev_egress.py remove --subnet "$DEV_LAN_SUBNET"`, then remove the dedicated bridge. Its isolated container and `${DEV_AIO_ROOT}` data can then be removed after the private backup and test findings are retained. Deleting the Dev stack never touches production mounts.

No production backup, database, credentials or recordings belong in this repository or public CI artifacts.
