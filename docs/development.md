# Development and deployment

## Local checks

Run `python3 -m unittest discover -s tests -v`, `python3 -m compileall -q catchuparr scripts tests`, and `ruff check .`. Build the importable archive with `python3 scripts/build_plugin.py`.

## Installing the plugin

Import `dist/catchuparr-<version>.zip` from Dispatcharr's Plugins page and enable it after inspecting its settings. The plugin code is installed under Dispatcharr's `/data/plugins/catchuparr`. The AIO container runs web and Celery with the same `/data/catchuparr` archive mount. During an update, stop the recorders, install the new ZIP, reload plugins, run the compatibility check, and resume the selected channels. Keep the prior ZIP and archive snapshot until playback smoke tests pass.

The first compatibility target is Dispatcharr v0.31.0. Any custom or later image requires the adapter signature and request tests before enabling recording or XC output.

The first active client path is the authenticated M3U/XMLTV output with HLS
archive playback. The XC adapter currently contains version-checked wrappers
and unit tests, but is **not installed at runtime**. Dispatcharr's XC timeshift
endpoint serves `.ts` with byte-range seeking; advertising local XC archive
before a matching authenticated TS/Range response and shared session-limit
policy exists would expose listings that cannot play. Complete those callbacks
and test actual TiviMate XC requests before enabling the hooks.

## Isolated development stack

Never mount production directories into Dev. Keep host-specific names, addresses and source backup paths in private deployment records outside this repository.

1. Inspect production read-only and pin its **AIO** image digest. Prefer an existing Dispatcharr backup ZIP under the private `containerfiles` tree. Copy it into a private Dev staging directory, check its SHA-256 and ZIP integrity, and verify `metadata.json` says `format=dispatcharr-backup`, `version=2`, `database_type=postgresql`. The ZIP contains the database; copy required logos, M3U and EPG files separately. Never copy recordings, a live PostgreSQL data directory, JWT material or credentials into source control.
2. Set `DEV_AIO_ROOT`, `DEV_BIND_IP` and `DISPATCHARR_DEV_AIO_IMAGE` in a private environment file. Create an empty `${DEV_AIO_ROOT}/data` and `${DEV_AIO_ROOT}/archive` owned by UID/GID 1000. Copy the ZIP into `${DEV_AIO_ROOT}/data/backups` **as a private copy**, without bind-mounting the production backup directory. Create a dedicated bridge with an unused private `DEV_LAN_SUBNET`, for example `sudo docker network create --driver bridge --subnet "$DEV_LAN_SUBNET" catchuparr-dev-lan`. Run `sudo python3 scripts/dev_egress.py apply --subnet "$DEV_LAN_SUBNET"` and then `check`. This initially denies all new Dev egress while allowing established replies. Do not start Dev if `check` fails.
3. Start `deploy/dev/compose.yml` as `catchuparr-dev`. It runs the same AIO image as production: embedded PostgreSQL, Redis, web and Celery in **one container**. `catchuparr-dev-lan` is external, so Compose cannot create an unrestricted replacement. Confirm that port 15656 binds only to the chosen Dev address and the container has only Dev mounts. Create the initial Dev admin account through the local UI. Before restoring, pause **only the Dev Celery Beat process** with `SIGSTOP`; keep the Celery worker running to execute the API restore. Record the Beat PID and verify it is stopped. The Dev egress block remains active throughout.
4. Restore using an authenticated **Dev admin** request: `POST /api/backups/<copied-zip-filename>/restore/`. In Dispatcharr v0.31.0, this route requires `IsAdmin`, returns HTTP 202 with `task_id` and `task_token`, and runs the restore in Celery. The backup must already be in Dev `/data/backups`; `POST /api/backups/upload/` is an alternative to copying it. Check completion at `GET /api/backups/status/<task_id>/?token=<task_token>` or through the Dev admin UI. Keep the token private; the restore can invalidate the initial admin session. A completed API task is still followed by a database content check. Do not assume elapsed time means success.
5. While Beat remains stopped, apply `deploy/dev/scrub.sql` to **Dev** PostgreSQL (`docker exec -i catchuparr-dev-aio psql -U dispatch -d dispatcharr -v ON_ERROR_STOP=1 < deploy/dev/scrub.sql`). Verify zero enabled periodic tasks, provider accounts, EPG sources and plugins. The scrub disables DVR rules, recording jobs, provider refreshes, notifications and integrations too. Then send `SIGCONT` to the recorded Beat PID and restart only the Dev AIO container. Recheck the zero counts and an unauthenticated-denial endpoint. Select one test channel only after agreeing on its source connection and tuner usage.
6. Before enabling a recorder, agree on a numeric `DEV_VU_IP` and `DEV_VU_PORT`. Stop Dev AIO; run `sudo python3 scripts/dev_egress.py remove --subnet "$DEV_LAN_SUBNET"`, then `apply` and `check` with `--vu-ip "$DEV_VU_IP" --vu-port "$DEV_VU_PORT"`; restart Dev. The rule permits established replies and new TCP connections only to that Vu+ endpoint. If the endpoint redirects elsewhere, leave it blocked and revise the allowlist explicitly.
7. Capture the installed TiviMate version/device. Test XC and M3U/XMLTV separately: archive icon, live start-over, repeated seeks, pause/resume, programme boundary, service restart, retention and rollback. Record HTTP method, URL template, time arguments, Range headers and response codes without logging credentials.

The current Dev instance was prepared from a consistent custom-format `pg_dump` before this API preference was clarified. For a future rebuild, the verified Dispatcharr ZIP and API flow above is the default. If no compatible ZIP exists, use `pg_dump -Fc`, verify it with `pg_restore -l` in a networkless container, restore into an isolated temporary PostgreSQL 17 container, apply `scrub.sql`, stop it, and copy only its cold cluster into the AIO Dev `/data/db` path (UID/GID 1000). A cold cluster prepared under a different glibc version may need `REINDEX DATABASE` for each copied database, followed by `ALTER DATABASE ... REFRESH COLLATION VERSION` inside the AIO image. Never mount a production database directory into Dev.

After stopping the Dev AIO container, remove the egress rule with `sudo python3 scripts/dev_egress.py remove --subnet "$DEV_LAN_SUBNET"`, then remove the dedicated bridge. Its isolated container and `${DEV_AIO_ROOT}` data can then be removed after the private backup and test findings are retained. Deleting the Dev stack never touches production mounts.

No production backup, database, credentials or recordings belong in this repository or public CI artifacts.
