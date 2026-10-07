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
    "plugin.py",
    "compatibility.py",
    "runtime.py",
    "tasks.py",
    "views.py",
    "security.py",
    "http.py",
    "adapters/__init__.py",
    "adapters/m3u.py",
    "adapters/xc.py",
    "engine/__init__.py",
    "engine/leases.py",
    "engine/playlist.py",
    "engine/recorder.py",
    "engine/store.py",
)


def build() -> Path:
    manifest = json.loads((PLUGIN / "plugin.json").read_text(encoding="utf-8"))
    version = manifest["version"]
    if not (PLUGIN / "plugin.py").is_file():
        raise RuntimeError("plugin.py is missing")
    missing = [name for name in REQUIRED_MODULES if not (PLUGIN / name).is_file()]
    if missing:
        raise RuntimeError(f"Required plugin modules are missing: {', '.join(missing)}")
    files = sorted(
        p for p in PLUGIN.rglob("*") if p.is_file() and p.suffix in {".py", ".json"}
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
