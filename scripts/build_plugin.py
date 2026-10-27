"""Build the folder-shaped ZIP accepted by Dispatcharr's plugin importer."""

from __future__ import annotations

import json
import sys
from pathlib import Path
from zipfile import ZIP_DEFLATED, ZipFile

ROOT = Path(__file__).resolve().parents[1]
PLUGIN = ROOT / "catchuparr"
DIST = ROOT / "dist"
REQUIRED_MODULES = (
    "__init__.py",
    "action_messages.py",
    "catchup_request.py",
    "plugin.py",
    "compatibility.py",
    "configuration.py",
    "source_rules.py",
    "schedule.py",
    "recorder_control.py",
    "logging_utils.py",
    "recorder_proxy.py",
    "runtime.py",
    "sharing_authority_outbox.py",
    "sharing_coordinator.py",
    "sharing_episode.py",
    "sharing_persistence.py",
    "sharing_planner.py",
    "sharing_recovery.py",
    "sharing_runtime.py",
    "stats.py",
    "tasks.py",
    "views.py",
    "security.py",
    "native_output_security.py",
    "native_output.py",
    "http.py",
    "ts_http.py",
    "xc_runtime.py",
    "adapters/__init__.py",
    "adapters/m3u.py",
    "adapters/xc.py",
    "adapters/recorder_proxy.py",
    "adapters/sharing_borrow_route.py",
    "adapters/sharing_native.py",
    "adapters/sharing_reader.py",
    "engine/__init__.py",
    "engine/admission.py",
    "engine/leases.py",
    "engine/playlist.py",
    "engine/recorder.py",
    "engine/store.py",
    "engine/ts_timeline.py",
)
REQUIRED_ASSETS = ("logo.png",)


def build() -> Path:
    manifest = json.loads((PLUGIN / "plugin.json").read_text(encoding="utf-8"))
    version = manifest["version"]
    if not (PLUGIN / "plugin.py").is_file():
        raise RuntimeError("plugin.py is missing")
    missing = [name for name in REQUIRED_MODULES if not (PLUGIN / name).is_file()]
    if missing:
        raise RuntimeError(f"Required plugin modules are missing: {', '.join(missing)}")
    missing_assets = [name for name in REQUIRED_ASSETS if not (PLUGIN / name).is_file()]
    if missing_assets:
        raise RuntimeError(f"Required plugin assets are missing: {', '.join(missing_assets)}")
    files = sorted(
        p for p in PLUGIN.rglob("*")
        if p.is_file() and p.suffix in {".py", ".json", ".png"}
    )
    DIST.mkdir(exist_ok=True)
    result = DIST / f"catchuparr-{version}.zip"
    with ZipFile(result, "w", compression=ZIP_DEFLATED) as archive:
        for path in files:
            archive.write(path, path.relative_to(ROOT))
    return result


if __name__ == "__main__":
    print(build())
    sys.exit(0)
