# Development and deployment

## Local checks

Run `python3 -m unittest discover -s tests -v`, `python3 -m compileall -q catchuparr scripts tests`, and `ruff check .`. Build the importable archive with `python3 scripts/build_plugin.py`.

## Installing the plugin

Import `dist/catchuparr-<version>.zip` from Dispatcharr's Plugins page and enable it after inspecting its settings. The plugin code is installed under Dispatcharr's `/data/plugins/catchuparr`. Web and Celery processes need the same `/data/catchuparr` archive mount. During an update, stop the recorders, install the new ZIP, reload plugins, run the compatibility check, and resume the selected channels. Keep the prior ZIP and archive snapshot until playback smoke tests pass.

The first compatibility target is Dispatcharr v0.31.0. Any custom or later image requires the adapter signature and request tests before enabling recording or XC output.

## Isolated development stack

Never mount production directories into Dev. Keep host-specific names, addresses and source backup paths in private deployment records outside this repository.

1. Via the Portainer API, inspect the production stack and pin the exact image digest. Verify the source PostgreSQL cluster is shut down cleanly, then make a private, read-only cold snapshot of its `/data` tree. Record checksums and restore a copy in a temporary isolated PostgreSQL 17 instance for a logical `pg_dump`.
2. Set `DEV_DATA_ROOT`, `DEV_BIND_IP`, `DEV_POSTGRES_PASSWORD` and a pinned `DISPATCHARR_DEV_IMAGE` in Portainer. Deploy `compose.bootstrap.yml` as `catchuparr-dev`. Restore the logical dump into its own PostgreSQL 17 database. Copy only required non-recording application files into `${DEV_DATA_ROOT}/data`; initialize empty `recordings` and `archive` directories. Keep the source snapshot unchanged.
3. In the copied database, disable DVR rules, recording jobs, provider refresh schedules, notifications, automation plugins and all providers except the explicitly selected Vu+ ARD test channel. Check the resulting rows before starting Celery. The bootstrap network is internal and has no external traffic.
4. Update that Portainer stack to `compose.runtime.yml`, using a pinned image compatible with the restored schema. Check port 15656 again, verify the web and worker use only Dev database/Redis/volumes, then perform an unauthenticated-denial smoke test before enabling the recorder. Dev has no production network or volume attachments. Allow outbound access only to the agreed Vu+ endpoint for the first live test.
5. Capture the installed TiviMate version/device. Test XC and M3U/XMLTV separately: archive icon, live start-over, repeated seeks, pause/resume, programme boundary, service restart, retention and rollback. Record HTTP method, URL template, time arguments, Range headers and response codes without logging credentials.

The Dev stack is removed through the Portainer API; its isolated containers, networks and `${DEV_DATA_ROOT}` data can then be removed after the private backup and test findings are retained. Deleting the Dev stack never touches production mounts.

No production backup, database, credentials or recordings belong in this repository or public CI artifacts.
