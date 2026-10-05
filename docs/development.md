# Development and deployment

## Local checks

Run `python3 -m unittest discover -s tests -v`, `python3 -m compileall -q catchuparr scripts tests`, and `ruff check .`. Build the importable archive with `python3 scripts/build_plugin.py`.

## Installing the plugin

Import `dist/catchuparr-<version>.zip` from Dispatcharr's Plugins page and enable it after inspecting its settings. The plugin code is installed under Dispatcharr's `/data/plugins/catchuparr`. Web and Celery processes need the same `/data/catchuparr` archive mount. During an update, stop the recorders, install the new ZIP, reload plugins, run the compatibility check, and resume the selected channels. Keep the prior ZIP and archive snapshot until playback smoke tests pass.

The first compatibility target is Dispatcharr v0.31.0. Any custom or later image requires the adapter signature and request tests before enabling recording or XC output.

## Isolated development stack

Never mount production directories into Dev. Keep host-specific names, addresses and source backup paths in private deployment records outside this repository.

1. Inspect the production stack read-only and pin its image digest. When its PostgreSQL cluster is running, create a consistent custom-format `pg_dump` through its local database socket into a private directory outside this repository. Validate it with `pg_restore -l` in a separate network-less container and record a SHA-256 checksum. Copy required non-database files to the same private backup; never copy a live PostgreSQL data directory. If production is stopped, a cold snapshot is also possible.
2. Set `DEV_DATA_ROOT`, `DEV_BIND_IP`, `DEV_POSTGRES_PASSWORD` and a pinned `DISPATCHARR_DEV_IMAGE` in private deployment configuration. Deploy `compose.bootstrap.yml` as `catchuparr-dev`. Restore the logical dump into its own PostgreSQL 17 database. Copy only required non-recording application files into `${DEV_DATA_ROOT}/data`; initialize empty `recordings` and `archive` directories. Keep the source backup unchanged.
3. In the copied database, disable DVR rules, recording jobs, provider refresh schedules, notifications, automation plugins and all providers except the explicitly selected Vu+ ARD test channel. Check the resulting rows before starting Celery. The bootstrap network is internal and has no external traffic.
4. Before attaching a runtime container, create a dedicated bridge with an unused private `DEV_LAN_SUBNET`, for example `sudo docker network create --driver bridge --subnet "$DEV_LAN_SUBNET" catchuparr-dev-lan`. Set an approved numeric `DEV_VU_IP` and `DEV_VU_PORT` for the single test source. Run `sudo python3 scripts/dev_egress.py apply --subnet "$DEV_LAN_SUBNET" --vu-ip "$DEV_VU_IP" --vu-port "$DEV_VU_PORT"`, then run the same command with `check`. The script installs a source-subnet rule in Docker's `DOCKER-USER` chain: established replies and new TCP connections to the selected Vu+ endpoint are allowed; all other new Dev egress is rejected. Do not attach containers if `check` fails. The internal `private` network still carries Dev database and Redis traffic.
5. Update that Portainer stack to `compose.runtime.yml`, using a pinned image compatible with the restored schema. `catchuparr-dev-lan` is an external network, so Compose cannot create an unrestricted replacement. Check port 15656 again, verify web and worker use only Dev database/Redis/volumes, and perform an unauthenticated-denial smoke test before enabling the recorder. Dev has no production network or volume attachments. If the Vu+ endpoint redirects to another address or port, leave it blocked and revise the allowlist explicitly.
6. Capture the installed TiviMate version/device. Test XC and M3U/XMLTV separately: archive icon, live start-over, repeated seeks, pause/resume, programme boundary, service restart, retention and rollback. Record HTTP method, URL template, time arguments, Range headers and response codes without logging credentials.

After stopping the Dev runtime containers, remove the egress rule with `sudo python3 scripts/dev_egress.py remove --subnet "$DEV_LAN_SUBNET"`, then remove the dedicated bridge. The Dev stack is removed through the Portainer API; its isolated containers and `${DEV_DATA_ROOT}` data can then be removed after the private backup and test findings are retained. Deleting the Dev stack never touches production mounts.

No production backup, database, credentials or recordings belong in this repository or public CI artifacts.
