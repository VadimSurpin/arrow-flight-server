#!/usr/bin/env bash
set -euo pipefail
# Installs arrow-flight-starrocks-connector into the local Maven repo so the StarRocks fork's FE can depend on it.
repo_root="$(cd "$(dirname "$0")/.." && pwd)"
export PATH="$repo_root/tools/.local/env/bin:$PATH"
cd "$repo_root"
./mvnw -q install -DskipTests -pl flight-starrocks -am
