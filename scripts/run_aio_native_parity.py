"""Run only native archive parity in a disposable pinned Dispatcharr AIO."""

from __future__ import annotations

import argparse
import subprocess
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def _manage_shell_source(probe_path: str) -> str:
    """Build a shell payload that needs only built-ins in Django's exec scope."""
    return (
        "exec(compile(open(" + repr(probe_path) + ", 'rb').read(), "
        + repr(probe_path)
        + ", 'exec'))"
    )


def run():
    parser = argparse.ArgumentParser()
    parser.add_argument("--image", required=True)
    args = parser.parse_args()
    name = f"catchuparr-native-parity-{time.time_ns()}"

    def docker(*parts, **kwargs):
        return subprocess.run(["docker", *parts], check=True, **kwargs)

    try:
        docker(
            "run", "-d", "--name", name, "--network", "none",
            "--mount", "type=volume,destination=/data",
            "-e", "DISPATCHARR_ENV=aio",
            "-e", "CATCHUPARR_INTEGRATION_TEST=1",
            args.image,
            stdout=subprocess.DEVNULL,
        )
        deadline = time.monotonic() + 240
        while True:
            ready = subprocess.run(
                [
                    "docker", "exec", name, "/dispatcharrpy/bin/python", "-c",
                    "import os,runpy,socket,sys; from pathlib import Path; "
                    "os.environ['DJANGO_SECRET_KEY']=Path('/data/jwt').read_text().strip(); "
                    "sys.argv=['manage.py','migrate','--check']; "
                    "runpy.run_path('/app/manage.py',run_name='__main__'); "
                    "socket.create_connection(('127.0.0.1',9191),2).close()",
                ],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
            if ready.returncode == 0:
                break
            state = subprocess.check_output(
                ["docker", "inspect", name, "--format", "{{.State.Status}}"],
                text=True,
            ).strip()
            if state not in {"running", "created"}:
                raise RuntimeError(f"Disposable AIO stopped during startup: {state}")
            if time.monotonic() >= deadline:
                raise RuntimeError("Disposable AIO did not become ready within 240 seconds")
            time.sleep(3)

        docker("cp", str(ROOT / "catchuparr"), f"{name}:/data/plugins/catchuparr")
        docker(
            "cp",
            str(ROOT / "scripts/aio_native_playback_parity_probe.py"),
            f"{name}:/tmp/aio_native_playback_parity_probe.py",
        )
        docker("exec", name, "chown", "-R", "1000:1000", "/data/plugins")
        probe_path = "/tmp/aio_native_playback_parity_probe.py"
        shell_source = _manage_shell_source(probe_path)
        docker(
            "exec", name, "/dispatcharrpy/bin/python", "-c",
            "import os,sys; from pathlib import Path; "
            "os.environ['DJANGO_SECRET_KEY']=Path('/data/jwt').read_text().strip(); "
            f"sys.argv=['manage.py','shell','-c',{shell_source!r}]; "
            "exec(compile(Path('/app/manage.py').read_bytes(), '/app/manage.py', 'exec'))",
            timeout=180,
        )
    finally:
        subprocess.run(
            ["docker", "rm", "-f", "-v", name],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            check=False,
        )


if __name__ == "__main__":
    run()
