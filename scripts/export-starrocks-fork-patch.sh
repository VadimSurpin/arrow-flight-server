#!/usr/bin/env bash
# Exports the StarRocks fork's commits as one patch against the pinned upstream commit, so CI can rebuild the fork
# from stock StarRocks without a separate fork repository.
set -euo pipefail
repo_root="$(cd "$(dirname "$0")/.." && pwd)"
fork="${STARROCKS_FORK:-$repo_root/../starrocks}"
base="$(git -C "$fork" merge-base HEAD origin/branch-4.0 2>/dev/null || cat "$repo_root/starrocks-fork/BASE_SHA")"
mkdir -p "$repo_root/starrocks-fork"
echo "$base" > "$repo_root/starrocks-fork/BASE_SHA"
git -C "$fork" format-patch --stdout --binary "$base..HEAD" > "$repo_root/starrocks-fork/flight-connector.patch"
echo "base $base; $(git -C "$fork" rev-list --count "$base..HEAD") commits; $(wc -c < "$repo_root/starrocks-fork/flight-connector.patch") bytes"
