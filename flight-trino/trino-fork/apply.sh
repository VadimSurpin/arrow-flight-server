#!/usr/bin/env bash
set -euo pipefail
fork_dir="$(cd "$(dirname "$0")" && pwd)"
checkout="${1:?Usage: apply.sh /path/to/clean/trino-checkout}"
base="$(cat "$fork_dir/BASE_SHA")"
if [[ "$(git -C "$checkout" rev-parse HEAD)" != "$base" ]]; then
    echo "Expected Trino base $base; check out that commit before applying." >&2
    exit 1
fi
if [[ -n "$(git -C "$checkout" status --porcelain --untracked-files=normal)" ]]; then
    echo "Trino checkout must be clean before applying the patch." >&2
    exit 1
fi
git -C "$checkout" apply --check "$fork_dir/flight-connector.patch"
git -C "$checkout" apply "$fork_dir/flight-connector.patch"
echo "Applied partial aggregation SPI and planner patch to $checkout"
