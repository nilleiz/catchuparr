import ast
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path


class Aio051ProbeImportTests(unittest.TestCase):
    def test_native_shell_probe_imports_plugin_from_plugin_manager_root(self):
        repository = Path(__file__).resolve().parents[1]
        probe_path = repository / "scripts/aio_051_configuration_probe.py"
        tree = ast.parse(probe_path.read_text(encoding="utf-8"))
        probe = next(
            node for node in tree.body
            if isinstance(node, ast.FunctionDef) and node.name == "probe"
        )
        path_setup = next(
            node for node in probe.body
            if isinstance(node, ast.Expr)
            and isinstance(node.value, ast.Call)
            and ast.unparse(node.value) == "sys.path.insert(0, '/data/plugins')"
        )
        catchuparr_import = next(
            node for node in probe.body
            if isinstance(node, ast.ImportFrom)
            and node.module is not None and node.module.startswith("catchuparr.")
        )
        self.assertLess(path_setup.lineno, catchuparr_import.lineno)

        with tempfile.TemporaryDirectory() as temporary:
            plugin_root = Path(temporary)
            package = plugin_root / "catchuparr"
            package.mkdir()
            (package / "__init__.py").write_text("", encoding="utf-8")
            (package / "configuration.py").write_text(
                "IMPORT_ORIGIN = 'plugin-manager-root'\n", encoding="utf-8"
            )
            path_setup.value.args[1] = ast.Constant(value=str(plugin_root))
            namespace = {
                "__name__": "__main__",
                "__file__": str(probe_path),
                "__package__": None,
            }
            source = (
                "import sys; namespace['sys']=sys; "
                f"exec(compile({ast.unparse(path_setup)!r}, '<probe-import>', 'exec'), namespace); "
                "from catchuparr.configuration import IMPORT_ORIGIN; "
                "print(IMPORT_ORIGIN)"
            )
            result = subprocess.run(
                [sys.executable, "-c", f"namespace={namespace!r}; {source}"],
                cwd=repository,
                capture_output=True,
                text=True,
                check=False,
            )

        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual("plugin-manager-root", result.stdout.strip())


if __name__ == "__main__":
    unittest.main()
