#!/usr/bin/env python3
"""Prepare pinned Trino sources for the patched-trino test profile."""

from pathlib import Path
import subprocess
import urllib.request


def main():
    """Download original files and apply the engine patch in the build directory."""
    fork = Path(__file__).resolve().parent
    module = fork.parent
    destination = module / "target" / "trino-patch-sources"
    destination.mkdir(parents=True, exist_ok=True)
    patch = fork / "flight-connector.patch"
    base = (fork / "BASE_SHA").read_text().strip()
    entries = patch.read_text().split("diff --git ")[1:]
    for entry in entries:
        relative = entry.splitlines()[0].split(" b/", 1)[1]
        target = (destination / relative).resolve()
        if not target.is_relative_to(destination.resolve()):
            raise ValueError("Patch path escapes the generated source directory")
        target.parent.mkdir(parents=True, exist_ok=True)
        if "\nnew file mode " in entry:
            if target.exists():
                target.unlink()
            continue
        url = f"https://raw.githubusercontent.com/trinodb/trino/{base}/{relative}"
        with urllib.request.urlopen(url, timeout=60) as response:
            target.write_bytes(response.read())
    subprocess.run(["git", "init", "-q", str(destination)], check=True)
    subprocess.run(["git", "-C", str(destination), "apply", "--check", str(patch)], check=True)
    subprocess.run(["git", "-C", str(destination), "apply", str(patch)], check=True)
    print(f"Prepared Trino {base} in {destination}")


if __name__ == "__main__":
    main()
