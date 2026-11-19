#!/usr/bin/env python3
"""Overlay compiled pinned engine changes onto the official Trino 446 runtime jars."""

import argparse
from pathlib import Path, PurePosixPath
import re
import zipfile


def patch_runtime(patch, classes, stock, output):
    """Preserve official jar entries except the changed outer classes and their nested classes."""
    modules = {"trino-main": [], "trino-spi": []}
    for relative in re.findall(r"^diff --git a/[^ ]+ b/([^\n]+)$", patch.read_text(), re.MULTILINE):
        match = re.fullmatch(r"core/(trino-main|trino-spi)/src/main/java/(io/trino/[A-Za-z0-9_/$]+)\.java", relative)
        if not match:
            raise ValueError(f"Unsupported engine patch path: {relative}")
        modules[match[1]].append(match[2])
    if not all(modules.values()):
        raise ValueError("Expected changes to both pinned engine jars")
    output.mkdir(parents=True, exist_ok=True)
    for module, outers in modules.items():
        replacements = {}
        for outer in outers:
            base = classes.joinpath(*PurePosixPath(outer).parts)
            direct = base.with_suffix(".class")
            if not direct.is_file():
                raise ValueError(f"Missing compiled engine class: {direct}")
            for compiled in [direct, *base.parent.glob(base.name + "$*.class")]:
                replacements[compiled.relative_to(classes).as_posix()] = compiled.read_bytes()
        name = f"{module}-446.jar"
        with zipfile.ZipFile(stock / name) as source, zipfile.ZipFile(output / name, "w", zipfile.ZIP_DEFLATED) as target:
            for entry in source.infolist():
                if entry.filename.startswith("META-INF/") and entry.filename.endswith((".SF", ".RSA", ".DSA")):
                    raise ValueError("Cannot overlay signed engine jars")
                if entry.filename.endswith(".class") and any(
                        entry.filename == outer + ".class" or entry.filename.startswith(outer + "$") for outer in outers):
                    continue
                target.writestr(entry, source.read(entry))
            for relative, data in sorted(replacements.items()):
                target.writestr(relative, data)


def main():
    """Patch the two stock engine jars from the prepared, compiled source changes."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--patch", required=True, type=Path)
    parser.add_argument("--classes", required=True, type=Path)
    parser.add_argument("--stock", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    patch_runtime(args.patch, args.classes, args.stock, args.output)


if __name__ == "__main__":
    main()
