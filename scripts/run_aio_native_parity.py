"""Run only native archive parity in a disposable pinned Dispatcharr AIO."""

from __future__ import annotations

import argparse
import subprocess
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def _manage_script_exec(script_path: str) -> str:
    """Use the same shell entrypoint and globals as the complete AIO probe."""
    return (
        f"filename={script_path!r}; "
        "namespace={'__name__':'__main__','__file__':filename,'__cached__':None,"
        "'__doc__':None,'__loader__':None,'__package__':None,'__spec__':None}; "
        "exec(compile(Path(filename).read_bytes(),filename,'exec'),namespace)"
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
        docker("exec", name, "chown", "-R", "1000:1000", "/data/plugins")
        with (ROOT / "scripts/aio_native_playback_parity_probe.py").open("rb") as script:
            docker(
                "exec", "-i", name, "/dispatcharrpy/bin/python", "-c",
                "import os,sys; from pathlib import Path; "
                "os.environ['DJANGO_SECRET_KEY']=Path('/data/jwt').read_text().strip(); "
                "sys.argv=['gunicorn','shell']; "
                + _manage_script_exec("/app/manage.py"),
                stdin=script,
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
