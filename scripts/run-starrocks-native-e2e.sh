#!/usr/bin/env bash
# End-to-end check of the native `flight` catalog: starts this repo's Flight SQL server on the test data and the
# StarRocks fork's FE (and BE with --with-be), then runs scripts/starrocks_native_e2e.py.
#
#   ./scripts/run-starrocks-native-e2e.sh            # FE only: catalog, metadata, types, credential handling
#   ./scripts/run-starrocks-native-e2e.sh --with-be  # also SELECT / EXPLAIN (needs a built BE in the fork)
#
# Needs: the fork built with scripts/build-starrocks-fork.sh (output/fe, and output/be for --with-be), the
# toolchain under tools/.local (JDK 17 env, JDK 21 for flight-server, venv with pymysql).
set -euo pipefail

repo_root="$(cd "$(dirname "$0")/.." && pwd)"
tools="$repo_root/tools/.local"
fork="${STARROCKS_FORK:-$repo_root/../starrocks}"
with_be=0
[ "${1:-}" = "--with-be" ] && with_be=1

fe_home="$fork/output/fe"
be_home="$fork/output/be"
[ -x "$fe_home/bin/start_fe.sh" ] || { echo "Build the fork first: scripts/build-starrocks-fork.sh --fe" >&2; exit 2; }
if [ "$with_be" = 1 ] && [ ! -x "$be_home/bin/start_be.sh" ]; then
    echo "--with-be needs a built BE in $be_home (scripts/build-starrocks-fork.sh --be, needs an AVX2 host)" >&2
    exit 2
fi

work="$(mktemp -d)"
flight_pid=""
cleanup() {
    [ -n "$flight_pid" ] && kill "$flight_pid" 2>/dev/null || true
    JAVA_HOME="$tools/env" "$fe_home/bin/stop_fe.sh" >/dev/null 2>&1 || true
    [ "$with_be" = 1 ] && JAVA_HOME="$tools/env" "$be_home/bin/stop_be.sh" >/dev/null 2>&1 || true
    rm -rf "$work"
}
trap cleanup EXIT

# 1. Flight SQL server (JDK 21) over the repo's test data
(
    export JAVA_HOME="$tools/jdk21" PATH="$tools/jdk21/bin:$tools/env/bin:$PATH"
    cd "$repo_root"
    ./mvnw -q test-compile -pl flight-server -am
    ./mvnw -q dependency:build-classpath -pl flight-server -Dmdep.outputFile="$work/cp.txt" -Dmdep.includeScope=test
)
opens=$(sed -n '/<argLine>/,/<\/argLine>/p' "$repo_root/flight-server/pom.xml" | grep -E -- '--add-opens' | tr -d ' ' | tr '\n' ' ')
(
    cd "$repo_root/flight-server"
    # shellcheck disable=SC2086
    exec "$tools/jdk21/bin/java" $opens -cp "target/test-classes:target/classes:$(cat "$work/cp.txt")" \
        pro.surpin.data.arrowflight.server.e2e.StandaloneTestFlightServer src/test/resources/test_db
) > "$work/flight.log" 2>&1 &
flight_pid=$!
for _ in $(seq 1 120); do grep -q '^FLIGHT_PORT=' "$work/flight.log" && break; sleep 1; done
flight_port="$(grep -m1 '^FLIGHT_PORT=' "$work/flight.log" | cut -d= -f2)"
[ -n "$flight_port" ] || { echo "Flight server did not start; see $work/flight.log" >&2; exit 1; }

# 2. StarRocks FE (+ BE)
export JAVA_HOME="$tools/env" PATH="$tools/env/bin:$PATH"
"$fe_home/bin/start_fe.sh" --daemon
if [ "$with_be" = 1 ]; then
    "$be_home/bin/start_be.sh" --daemon
fi
for _ in $(seq 1 90); do
    "$tools/venv/bin/python" -c "import pymysql; pymysql.connect(host='127.0.0.1', port=9030, user='root', connect_timeout=2)" 2>/dev/null && break
    sleep 2
done
if [ "$with_be" = 1 ]; then
    "$tools/venv/bin/python" - <<'PY'
import pymysql, time
c = pymysql.connect(host="127.0.0.1", port=9030, user="root", autocommit=True)
cur = c.cursor()
try:
    cur.execute('ALTER SYSTEM ADD BACKEND "127.0.0.1:9050"')
except pymysql.MySQLError:
    pass
for _ in range(90):
    cur.execute("SHOW BACKENDS")
    if any(row[8] == "true" for row in cur.fetchall()):
        break
    time.sleep(2)
PY
fi

# 3. Scenario
extra=()
[ "$with_be" = 1 ] && extra+=(--with-be)
"$tools/venv/bin/python" "$repo_root/scripts/starrocks_native_e2e.py" "$flight_port" "${extra[@]}"
