"""Start and clean up a disposable AIO for synthetic compatibility checks."""

import argparse
import subprocess
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def run():
    parser = argparse.ArgumentParser()
    parser.add_argument("--image", required=True)
    args = parser.parse_args()
    name = f"catchuparr-ci-{time.time_ns()}"

    def docker(*parts, **kwargs):
        return subprocess.run(["docker", *parts], check=True, **kwargs)

    try:
        docker("run", "-d", "--name", name, "--network", "none",
               "-e", "DISPATCHARR_ENV=aio", "-e", "CATCHUPARR_INTEGRATION_TEST=1",
               args.image, stdout=subprocess.DEVNULL)
        deadline = time.monotonic() + 240
        while True:
            ready = subprocess.run(
                ["docker", "exec", name, "/dispatcharrpy/bin/python", "-c",
                 "import socket; socket.create_connection(('127.0.0.1',9191),2).close()"],
                stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
            )
            if ready.returncode == 0:
                break
            if time.monotonic() >= deadline:
                raise RuntimeError("Disposable AIO did not become ready within 240 seconds")
            time.sleep(3)
        docker("cp", str(ROOT / "catchuparr"), f"{name}:/data/plugins/catchuparr")
        docker("exec", name, "chown", "-R", "1000:1000", "/data/plugins")
        with (ROOT / "scripts/aio_integration_probe.py").open("rb") as script:
            docker("exec", "-i", name, "/dispatcharrpy/bin/python", "/app/manage.py",
                   "shell", stdin=script)
    finally:
        subprocess.run(["docker", "rm", "-f", "-v", name],
                       stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, check=False)


if __name__ == "__main__":
    run()
