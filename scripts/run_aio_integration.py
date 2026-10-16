"""Start and clean up a disposable AIO for synthetic compatibility checks."""

import argparse
import json
import subprocess
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
# The full probe runs its native teardown gates serially. Thirteen waits each
# include the configured disconnect/cleanup phases, a 90-second stopping-marker
# allowance, and monitor margin (at least 1,235 seconds total), before its
# bounded media and recorder-failover phases.
FULL_AIO_PROBE_TIMEOUT_SECONDS = 1800


def _manage_script_exec(script_path: str) -> str:
    """Build a runpy-free entrypoint that preserves the selected process argv."""
    return (
        f"filename={script_path!r}; "
        "namespace={'__name__':'__main__','__file__':filename,'__cached__':None,"
        "'__doc__':None,'__loader__':None,'__package__':None,'__spec__':None}; "
        "exec(compile(Path(filename).read_bytes(),filename,'exec'),namespace)"
    )


def run():
    parser = argparse.ArgumentParser()
    parser.add_argument("--image", required=True)
    parser.add_argument(
        "--configuration-only",
        action="store_true",
        help="Run source-rule unit tests and the provider-free Django configuration probe, then stop.",
    )
    args = parser.parse_args()
    name = f"catchuparr-ci-{time.time_ns()}"

    def docker(*parts, **kwargs):
        return subprocess.run(["docker", *parts], check=True, **kwargs)

    try:
        docker("run", "-d", "--name", name, "--network", "none",
               "--mount", "type=volume,destination=/data",
               "-e", "DISPATCHARR_ENV=aio", "-e", "CATCHUPARR_INTEGRATION_TEST=1",
               args.image, stdout=subprocess.DEVNULL)
        deadline = time.monotonic() + 240
        while True:
            ready = subprocess.run(
                ["docker", "exec", name, "/dispatcharrpy/bin/python", "-c",
                 "import os,runpy,socket,sys; from pathlib import Path; "
                 "os.environ['DJANGO_SECRET_KEY']=Path('/data/jwt').read_text().strip(); "
                 "sys.argv=['manage.py','migrate','--check']; "
                 "runpy.run_path('/app/manage.py',run_name='__main__'); "
                 "socket.create_connection(('127.0.0.1',9191),2).close()"],
                stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
            )
            if ready.returncode == 0:
                break
            state = subprocess.check_output(
                ["docker", "inspect", name, "--format", "{{.State.Status}}"], text=True,
            ).strip()
            if state not in {"running", "created"}:
                raise RuntimeError(f"Disposable AIO stopped during startup: {state}")
            if time.monotonic() >= deadline:
                raise RuntimeError("Disposable AIO did not become ready within 240 seconds")
            time.sleep(3)
        docker("cp", str(ROOT / "catchuparr"), f"{name}:/data/plugins/catchuparr")
        docker("cp", str(ROOT / "scripts/aio_recorder_media.py"), f"{name}:/tmp/aio_recorder_media.py")
        docker("cp", str(ROOT / "scripts/aio_stats_probe.py"), f"{name}:/tmp/aio_stats_probe.py")
        docker("cp", str(ROOT / "scripts/aio_native_output_probe.py"),
               f"{name}:/tmp/aio_native_output_probe.py")
        docker("cp", str(ROOT / "scripts/aio_stats_gevent_probe.py"),
               f"{name}:/tmp/aio_stats_gevent_probe.py")
        docker("cp", str(ROOT / "scripts/aio_native_redis_diagnostic.py"),
               f"{name}:/tmp/aio_native_redis_diagnostic.py")
        docker("cp", str(ROOT / "scripts/aio_051_configuration_probe.py"),
               f"{name}:/tmp/aio_051_configuration_probe.py")
        docker("cp", str(ROOT / "scripts/aio_sharing_registration_crash.py"),
               f"{name}:/tmp/aio_sharing_registration_crash.py")
        docker("cp", str(ROOT / "scripts/aio_recorder_failover.py"), f"{name}:/tmp/aio_recorder_failover.py")
        docker("exec", name, "chown", "-R", "1000:1000", "/data/plugins")
        docker("cp", str(ROOT / "tests"), f"{name}:/tmp/tests")
        docker("exec", "--workdir", "/tmp", "-e", "PYTHONPATH=/data/plugins:/tmp",
               name, "/dispatcharrpy/bin/python", "-m", "unittest",
               "tests.test_source_rules", "tests.test_schedule",
               "tests.test_engine_recorder", "tests.test_engine_playlist",
               "tests.test_engine_store", "-q", timeout=120)
        with (ROOT / "scripts/aio_051_configuration_probe.py").open("rb") as script:
            result = docker(
                "exec", "-i", name, "/dispatcharrpy/bin/python", "-c",
                "import os,sys; from pathlib import Path; "
                "os.environ['DJANGO_SECRET_KEY']=Path('/data/jwt').read_text().strip(); "
                "sys.argv=['gunicorn','shell']; "
                + _manage_script_exec("/app/manage.py"),
                stdin=script,
                stdout=subprocess.PIPE,
                text=True,
                timeout=120,
            )
        receipts = []
        for line in result.stdout.splitlines():
            try:
                receipt = json.loads(line)
            except ValueError:
                continue
            if isinstance(receipt, dict) and receipt.get("status") == "pass":
                receipts.append(receipt)
        if not receipts or receipts[-1].get("provider_connections") != 0:
            raise RuntimeError("Native 0.5.1 Django configuration probe did not pass cleanly")
        print(json.dumps(receipts[-1], sort_keys=True))
        if args.configuration_only:
            return
        with (ROOT / "scripts/aio_integration_probe.py").open("rb") as script:
            docker("exec", "-i", name, "/dispatcharrpy/bin/python", "-c",
                   "import os,sys; from pathlib import Path; "
                   "os.environ['DJANGO_SECRET_KEY']=Path('/data/jwt').read_text().strip(); "
                   # Simulate only Dispatcharr's recognized stream-worker process role.
                   # Django still executes its normal shell command; app startup sees
                   # argv[0] == gunicorn and follows the real ProxyConfig.ready path.
                   # This synthetic shell does not start a Gunicorn HTTP server.
                   "sys.argv=['gunicorn','shell']; "
                   + _manage_script_exec("/app/manage.py"),
                   # Sequential native cleanup, media, and recording failover
                   # phases have bounded inner waits within this derived cap.
                   stdin=script, timeout=FULL_AIO_PROBE_TIMEOUT_SECONDS)
    finally:
        subprocess.run(["docker", "rm", "-f", "-v", name],
                       stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, check=False)


if __name__ == "__main__":
    run()
