#!/usr/bin/env bash
# Wire-format matrix for the Flight scan: decimal form x batch compression, measured through StarRocks on queries that
# move whole tables over the wire (aggregation and join pushdown off). For every configuration the Flight pods are
# restarted with the options, the StarRocks FE is restarted (it caches pod addresses) and run.py is executed.
#
#   KUBECONFIG=... KUBECTL=.../kubectl PYTHON=.../python ./compression-matrix.sh [outdir]
#
# Options reach the servers through JAVA_OPTS: -DdecimalWire=arrow|narrow, -DflightCompression=none|zstd,
# -DflightCompressionLevel=N.
set -euo pipefail
KUBECTL=${KUBECTL:-kubectl}
PYTHON=${PYTHON:-python3}
NS=${NS:-tpch-bench}
QUERIES=${QUERIES:-1,3,18,21}
MODE=${MODE:-flight_noagg}
RUNS=${RUNS:-3}
OUT=${1:-$(dirname "$0")/results/compression-matrix}
BASE_OPTS="-Xmx2g -XX:MaxDirectMemorySize=6g -DduckDbMemoryLimit=3GB -DduckDbThreads=8 -DendpointsPerServer=3 -DarrowAllocatorLimitBytes=5000000000"
# name | extra JAVA_OPTS
CONFIGS=(
  "narrow-none|-DdecimalWire=narrow -DflightCompression=none"
  "arrow-none|-DdecimalWire=arrow -DflightCompression=none"
  "arrow-zstd1|-DdecimalWire=arrow -DflightCompression=zstd -DflightCompressionLevel=1"
  "arrow-zstd3|-DdecimalWire=arrow -DflightCompression=zstd -DflightCompressionLevel=3"
  "narrow-zstd1|-DdecimalWire=narrow -DflightCompression=zstd -DflightCompressionLevel=1"
)
mkdir -p "$OUT"

restart_pf() {
  pkill -f 'port-forward.*19030' 2>/dev/null || true
  sleep 2
  nohup "$KUBECTL" -n "$NS" port-forward svc/starrocks-fe 19030:9030 >/tmp/pf.log 2>&1 &
  sleep 6
}

for entry in "${CONFIGS[@]}"; do
  name=${entry%%|*}
  extra=${entry#*|}
  echo "=== $name: $extra"
  "$KUBECTL" -n "$NS" set env sts/flight "JAVA_OPTS=$BASE_OPTS $extra" >/dev/null
  "$KUBECTL" -n "$NS" delete pod flight-0 flight-1 flight-2 --wait=true >/dev/null
  until [ "$("$KUBECTL" -n "$NS" get pods | grep -E '^flight-[012] ' | grep -c '1/1')" = 3 ]; do sleep 5; done
  "$KUBECTL" -n "$NS" delete pod starrocks-fe-0 --wait=true >/dev/null
  until "$KUBECTL" -n "$NS" get pod starrocks-fe-0 2>/dev/null | grep -q '1/1'; do sleep 5; done
  sleep 20
  restart_pf
  ( while true; do date +%T; "$KUBECTL" -n "$NS" top pod 2>/dev/null | grep '^flight'; sleep 15; done ) >"$OUT/$name.mem.log" 2>&1 &
  sampler=$!
  "$PYTHON" "$(dirname "$0")/run.py" 127.0.0.1 19030 --modes "$MODE" --queries "$QUERIES" --runs "$RUNS" \
    --timeout 600 --out "$OUT/$name.json" | grep -v -i warning | tee "$OUT/$name.log" || true
  kill "$sampler" 2>/dev/null || true
done

"$PYTHON" - "$OUT" <<'PY'
import glob, os, re, sys
out = sys.argv[1]
rows = {}
names = []
for log in sorted(glob.glob(os.path.join(out, "*.log"))):
    if log.endswith(".mem.log"):
        continue
    name = os.path.basename(log)[:-4]
    names.append(name)
    for line in open(log):
        m = re.match(r"q(\d+) \S+\s+\[(.*?)\]\s*(.*)", line)
        if m:
            runs = [float(x) for x in m[2].split(",") if x.strip()]
            rows.setdefault(int(m[1]), {})[name] = (min(runs[1:] or runs) if runs else None, m[3][:30])
print("query  " + "  ".join(f"{n:>13}" for n in names))
for q in sorted(rows):
    cells = []
    for n in names:
        v = rows[q].get(n)
        cells.append(f"{v[0]:13.1f}" if v and v[0] is not None else f"{'FAIL':>13}")
    print(f"q{q:<5} " + "  ".join(cells))
for n in names:
    peak = 0
    for line in open(os.path.join(out, n + ".mem.log")):
        m = re.match(r"flight-\d\s+\S+\s+(\d+)Mi", line)
        if m:
            peak = max(peak, int(m[1]))
    print(f"peak pod memory {n}: {peak} MiB")
PY
