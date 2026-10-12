import ast
import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

from scripts.run_aio_integration import _manage_script_exec as _full_aio_manage_script_exec
from scripts.run_aio_native_parity import _manage_script_exec, _success_receipt


class NativeParityRunnerTests(unittest.TestCase):
    def test_uses_full_aio_manage_entrypoint_and_globals(self):
        with tempfile.TemporaryDirectory() as directory:
            probe = Path(directory) / "probe.py"
            probe.write_text("print('synthetic probe started')\n", encoding="utf-8")
            probe_exec = _manage_script_exec(str(probe))
            self.assertEqual(probe_exec, _full_aio_manage_script_exec(str(probe)))

            result = subprocess.run(
                [
                    sys.executable,
                    "-c",
                    "from pathlib import Path; " + probe_exec,
                ],
                capture_output=True,
                text=True,
                check=False,
            )

        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stdout.strip(), "synthetic probe started")

    def test_probe_plugin_path_resolves_modules_from_plugin_manager_root(self):
        probe_script = (
            Path(__file__).resolve().parents[1]
            / "scripts/aio_native_playback_parity_probe.py"
        )
        tree = ast.parse(probe_script.read_text(encoding="utf-8"))
        probe = next(
            node for node in tree.body
            if isinstance(node, ast.FunctionDef) and node.name == "probe"
        )
        path_insert = next(
            node for node in ast.walk(probe)
            if isinstance(node, ast.Call)
            and ast.unparse(node) == "sys.path.insert(0, '/data/plugins')"
        )
        with tempfile.TemporaryDirectory() as directory:
            plugin_root = Path(directory)
            package = plugin_root / "catchuparr"
            package.mkdir()
            (package / "__init__.py").write_text(
                "ORIGIN = 'plugin-manager-root'\n", encoding="utf-8"
            )
            path_insert.args[1] = ast.Constant(value=str(plugin_root))
            source = f"import sys; {ast.unparse(path_insert)}; "
            source += "import catchuparr; print(catchuparr.ORIGIN)"
            result = subprocess.run(
                [sys.executable, "-c", source],
                capture_output=True,
                text=True,
                check=False,
                cwd=Path(__file__).resolve().parents[1],
            )

        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stdout.strip(), "plugin-manager-root")

    def test_probe_call_runs_in_django_shell_namespace(self):
        probe_script = (
            Path(__file__).resolve().parents[1]
            / "scripts/aio_native_playback_parity_probe.py"
        )
        tree = ast.parse(probe_script.read_text(encoding="utf-8"))
        final_statement = tree.body[-1]
        self.assertIsInstance(final_statement, ast.Expr)
        self.assertEqual(ast.unparse(final_statement), "probe()")

        source = (
            "namespace={'__name__':'django_shell',"
            "'probe':lambda:print('probe invoked')}; "
            f"exec(compile({ast.unparse(final_statement)!r}, '<stdin>', 'exec'), namespace)"
        )
        result = subprocess.run(
            [sys.executable, "-c", source],
            capture_output=True,
            text=True,
            check=False,
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stdout.strip(), "probe invoked")

    def test_runner_rejects_missing_or_incomplete_success_receipt(self):
        with self.assertRaisesRegex(RuntimeError, "no structured success receipt"):
            _success_receipt("")
        with self.assertRaisesRegex(RuntimeError, "omitted one or more"):
            _success_receipt(
                '{"status":"pass","dispatcharr_version":"0.32.0","cases":{}}\n'
            )
        cases = {
            name: {
                "get_status": 404 if name == "empty" else 200,
                "head_status": 404 if name == "empty" else 200,
                "head_created_lease": False,
            }
            for name in ("current", "completed", "gapped", "empty")
        }
        receipt = {
            "status": "pass",
            "dispatcharr_version": "0.32.0",
            "cases": cases,
            "authorization": {
                "legacy_missing_token": 401,
                "native_invalid_capability": 403,
            },
        }
        self.assertEqual(_success_receipt(json.dumps(receipt)), receipt)


if __name__ == "__main__":
    unittest.main()
