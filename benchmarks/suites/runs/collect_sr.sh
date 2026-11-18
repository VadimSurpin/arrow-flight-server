#!/bin/bash
# StarRocks (native flight catalog) collection with the same protocol as the cache-off Spark collection: Flight pods restarted before every chunk,
# default run (flight only, 3 reps, 1 warmup, chunks of 5 queries), then the knob sweep (configs/starrocks.json, 2 reps, 4 warmups on TPC-H and 10 elsewhere,
# 1 query per chunk) on queries whose default median is under 120 s. Needs the FE port-forward on localhost:19030 (sr_up.sh). ClickBench is skipped
# (the catalog cannot read its unsigned 16-bit column); synthetic gets the default run only (its queries are ~1 s, too short for knob labels).
# Output runs/fresh-sr/<suite>-<layout>-<kind>.jsonl, markers runs/state/SR_<suite>_<layout>_<kind>.done, end marker SR_DONE in runs/collect_sr.log.
cd /home/vadim/arrowflight-server/ArrowFlight/benchmarks/suites
KC=../../tools/.local/bin/kubectl
export KUBECONFIG=/home/vadim/cloudru-k8s/.kube/kubeconfig-surpin.yaml
K="$KC -n tpch-bench"
S=runs/state
mkdir -p runs/fresh-sr
healthy() {
  [ "$($K get nodes --no-headers | grep -c NotReady)" = 0 ] && [ -z "$($K get pods --no-headers | grep Terminating)" ] \
    && [ "$($K get pods --no-headers | grep -E '^starrocks-(fe|be)' | grep -vc ' Running ')" = 0 ] && (exec 3<>/dev/tcp/127.0.0.1/19030) 2>/dev/null
}
pick() {  # file: queries whose default median is under 120 s
  python3 - $1 <<'P'
import json, statistics as st, sys, collections
d = collections.defaultdict(list)
for l in open(sys.argv[1]):
    x = json.loads(l)
    if x["type"] == "run" and not x["warmup"] and not x["error"] and x["valid"]:
        d[x["query"]].append(x["wall"])
print(",".join(q for q in sorted(d) if st.median(d[q]) < 120))
P
}
unit() {  # suite layout schema kind(default|phase1) configs chunk reps
  local suite=$1 l=$2 schema=$3 kind=$4 cfg=$5 n=$6 nreps=$7
  [ -e $S/SR_${suite}_${l}_$kind.done ] && return
  local wu=1; [ $kind = phase1 ] && { wu=4; [ $suite != tpch ] && wu=10; }
  local out=runs/fresh-sr/$suite-$l-$kind.jsonl
  local q=all; [ $kind = phase1 ] && q=$(pick runs/fresh-sr/$suite-$l-default.jsonl)
  [ -z "$q" ] && return
  echo "$(date +%T) sr $suite $l $kind" >> runs/collect_sr.log
  python3 chunks.py $suite "$q" $n | while read chunk; do
    [ $kind = phase1 ] && grep -q "\"query\": \"$chunk\"" $out 2>/dev/null && continue  # resume: single-query chunks already in the file are done
    until healthy; do sleep 30; done
    ./restart_flight.sh
    python3 harness.py run --engine starrocks --suite $suite --schema $schema --queries "$chunk" --modes flight --configs $cfg --reps $nreps --rep-start 1 --warmup $wu \
      --timeout 300 --kubectl $KC --no-features --out $out >> ${out%.jsonl}.log 2>&1
  done
  touch $S/SR_${suite}_${l}_$kind.done
}
for suite in tpch ssb tpcds; do
  for l in asis sorted shuffled; do
    unit $suite $l ${suite}_sf100_$l default configs/default.json 5 3
    unit $suite $l ${suite}_sf100_$l phase1 configs/starrocks.json 1 2
  done
done
unit taxi default taxi default configs/default.json 5 3
unit taxi default taxi phase1 configs/starrocks.json 1 2
while read schema; do unit synthetic $schema $schema default configs/default.json 8 3; done < runs/syn_schemas.txt
echo SR_DONE >> runs/collect_sr.log
