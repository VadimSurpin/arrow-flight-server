"""Check that runtime overlay preserves stock code and replaces only compiled patch classes."""

import importlib.util
from pathlib import Path
import tempfile
import unittest
import zipfile

SPEC = importlib.util.spec_from_file_location("patch_runtime", Path(__file__).resolve().parents[3] / "trino-fork/patch-runtime.py")
RUNTIME = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(RUNTIME)


class RuntimePatchTest(unittest.TestCase):
    """Validate the boundaries and nested-class replacement of the runtime overlay."""

    def test_replaces_changed_classes_and_preserves_other_entries(self):
        """Replace stale nested classes while retaining manifests and untouched classes."""
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            stock, classes, output = root / "stock", root / "classes", root / "output"
            stock.mkdir()
            patch = root / "engine.patch"
            paths = {"trino-main": "io/trino/metadata/Metadata", "trino-spi": "io/trino/spi/connector/ConnectorMetadata"}
            patch.write_text("".join(f"diff --git a/core/{module}/src/main/java/{outer}.java b/core/{module}/src/main/java/{outer}.java\n"
                                     for module, outer in paths.items()))
            for module, outer in paths.items():
                base = classes / outer
                base.parent.mkdir(parents=True, exist_ok=True)
                base.with_suffix(".class").write_bytes(b"new outer")
                base.with_name(base.name + "$New.class").write_bytes(b"new nested")
                with zipfile.ZipFile(stock / f"{module}-446.jar", "w") as jar:
                    jar.writestr(outer + ".class", b"old outer")
                    jar.writestr(outer + "$Old.class", b"stale nested")
                    jar.writestr("META-INF/MANIFEST.MF", b"unchanged manifest")
                    jar.writestr("untouched.class", b"unchanged code")
            RUNTIME.patch_runtime(patch, classes, stock, output)
            for module, outer in paths.items():
                with zipfile.ZipFile(output / f"{module}-446.jar") as jar:
                    self.assertEqual(b"new outer", jar.read(outer + ".class"))
                    self.assertEqual(b"new nested", jar.read(outer + "$New.class"))
                    self.assertNotIn(outer + "$Old.class", jar.namelist())
                    self.assertEqual(b"unchanged manifest", jar.read("META-INF/MANIFEST.MF"))
                    self.assertEqual(b"unchanged code", jar.read("untouched.class"))

    def test_rejects_paths_outside_the_pinned_engine_modules(self):
        """Reject overlays that include unrelated module paths."""
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            patch = root / "engine.patch"
            patch.write_text("diff --git a/other/File.java b/other/File.java\n")
            with self.assertRaisesRegex(ValueError, "Unsupported engine patch path"):
                RUNTIME.patch_runtime(patch, root, root, root / "output")


if __name__ == "__main__":
    unittest.main()
