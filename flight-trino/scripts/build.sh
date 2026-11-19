#!/usr/bin/env bash
set -euo pipefail
root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
module="$root/flight-trino"
cd "$root"
action="${1:-build}"
if (( $# )); then shift; fi
maven="${MAVEN_BIN:-mvn}"
reactor=(-f "$module/reactor/pom.xml" -pl :arrow-flight-trino-connector -am)
case "$action" in
    build)
        exec "$maven" "${reactor[@]}" package "$@"
        ;;
    integration)
        python3 "$root/trino-fork/prepare-tests.py"
        exec "$maven" "${reactor[@]}" -Pintegration,patched-trino test \
            -DexcludedGroups= '-Dgroups=unit | integration' '-Dtest=Trino*Test' \
            -Dsurefire.failIfNoSpecifiedTests=false "$@"
        ;;
    check)
        "$maven" compile checkstyle:check spotbugs:check "$@"
        "$maven" test "$@"
        "$maven" "${reactor[@]}" compile checkstyle:check spotbugs:check "$@"
        "$maven" "${reactor[@]}" test "$@"
        exec bash "$module/scripts/build.sh" integration "$@"
        ;;
    image)
        exec docker build -f "$root/trino-fork/Dockerfile" \
            -t "${TRINO_IMAGE:-arrowflight-trino-partial:446}" "$@" "$root"
        ;;
    run)
        exec docker compose --project-directory "$module" -f "$module/compose.yaml" up -d "$@" trino
        ;;
    down)
        exec docker compose --project-directory "$module" -f "$module/compose.yaml" down "$@"
        ;;
    query)
        exec python3 "$module/scripts/query_flight.py" "$@"
        ;;
    *)
        echo "Usage: build.sh {build|integration|check|image|run|down|query} [arguments]" >&2
        exit 2
        ;;
esac
