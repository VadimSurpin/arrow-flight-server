#!/bin/bash
# Cache-off collection for ClickBench (hits) and NYC taxi (yellow), same protocol as collect_nocache.sh: Flight pods restarted before every chunk,
# default run (direct+flight, 3 reps, 1 warmup, chunks of 5 queries), then the phase-1 knob sweep (flight, 2 reps, 13 configs, 1 query per chunk, 10 warmups)
# on the queries whose default Flight median is under 120 s. These suites have a single layout. Output runs/fresh-nocache/<suite>-<kind>.jsonl, markers runs/state/N_<suite>_<kind>.done.
cd /home/vadim/arrowflight-server/ArrowFlight/benchmarks/suites
KC=../../tools/.local/bin/kubectl
export KUBECONFIG=/home/vadim/cloudru-k8s/.kube/kubeconfig-surpin.yaml
K="$KC -n tpch-bench"
S=runs/state
healthy() {
  [ "$($K get nodes --no-headers | grep -c NotReady)" = 0 ] && [ -z "$($K get pods --no-headers | grep Terminating)" ]
}
pick() {  # suite: queries whose default Flight median is under 120 s
  python3 - $1 <<'E'
import json, statistics as st, sys, collections
d = collections.defaultdict(list)
for l in open("runs/fresh-nocache/%s-default.jsonl" % sys.argv[1]):
    x = json.loads(l)
    if x["type"] == "run" and x["mode"] == "flight" and not x["warmup"] and not x["error"] and x["valid"]:
        d[x["query"]].append(x["wall"])
print(",".join(q for q in sorted(d) if st.median(d[q]) < 120))
E
}
unit() {  # suite kind(phase1|default) modes configs chunk reps
  local suite=$1 kind=$2 modes=$3 cfg=$4 n=$5 nreps=$6
  [ -e $S/N_${suite}_$kind.done ] && return
  local wu=1; [ $kind = phase1 ] && wu=10
  local out=runs/fresh-nocache/$suite-$kind.jsonl
  local q
  if [ $kind = phase1 ]; then q=$(pick $suite); else q=all; fi
  [ -z "$q" ] && return
  echo "$(date +%T) fresh $suite $kind" >> runs/collect_cbtaxi.log
  python3 chunks.py $suite "$q" $n | while read chunk; do
    [ $kind = phase1 ] && grep -q "\"query\": \"$chunk\"" $out 2>/dev/null && continue  # resume: single-query chunks already in the file are done
    until healthy; do sleep 30; done
    ./restart_flight.sh
    python3 harness.py run --suite $suite --queries "$chunk" --modes $modes --configs $cfg --reps $nreps --rep-start 1 --warmup $wu \
      --timeout 300 --kubectl $KC --no-features --out $out >> ${out%.jsonl}.log 2>&1
  done
  touch $S/N_${suite}_$kind.done
}
for suite in taxi clickbench; do
  unit $suite default direct,flight configs/default.json 5 3
  unit $suite phase1 flight configs/phase1.json 1 2
done
echo CBTAXI_DONE >> runs/collect_cbtaxi.log
