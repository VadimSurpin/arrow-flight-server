#!/usr/bin/env bash
set -euo pipefail
repo_root="$(cd "$(dirname "$0")/.." && pwd)"
export PATH="$repo_root/tools/.local/env/bin:$PATH"
export JAVA_HOME="$repo_root/tools/.local/env"
fork="${STARROCKS_FORK:-$repo_root/../starrocks}"
cd "$fork"
exec ./build.sh "$@"
