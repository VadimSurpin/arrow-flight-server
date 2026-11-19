#!/bin/bash
# Cache-off collection for the synthetic datasets (runs/syn_schemas.txt, phase-2 grid of datasets/sweep.py), same protocol as collect_cbtaxi.sh:
# default run per schema (direct+flight, 3 reps, 1 warmup, all 8 queries in one chunk, Flight pods restarted before the chunk), then the phase-1 knob sweep
# (flight, 13 configs, 2 reps, 10 warmups, 1 query per chunk) on queries whose default Flight median is under 120 s.
# Output runs/fresh-nocache/synthetic-<kind>.jsonl (schema is a field of each record), markers runs/state/N_syn_<schema>_<kind>.done, end marker SYN_DONE in runs/collect_syn.log.
cd /home/vadim/arrowflight-server/ArrowFlight/benchmarks/suites
KC=../../tools/.local/bin/kubectl
export KUBECONFIG=/home/vadim/cloudru-k8s/.kube/kubeconfig-surpin.yaml
K="$KC -n tpch-bench"
S=runs/state
healthy() {
  [ "$($K get nodes --no-headers | grep -c NotReady)" = 0 ] && [ -z "$($K get pods --no-headers | grep Terminating)" ]
}
pick() {  # schema: queries whose default Flight median is under 120 s
  python3 - $1 <<'P'
import json, statistics as st, sys, collections
d = collections.defaultdict(list)
for l in open("runs/fresh-nocache/synthetic-default.jsonl"):
    x = json.loads(l)
    if x["type"] == "run" and x["schema"] == sys.argv[1] and x["mode"] == "flight" and not x["warmup"] and not x["error"] and x["valid"]:
        d[x["query"]].append(x["wall"])
print(",".join(q for q in sorted(d) if st.median(d[q]) < 120))
P
}
unit() {  # schema kind(phase1|default) modes configs chunk reps
  local schema=$1 kind=$2 modes=$3 cfg=$4 n=$5 nreps=$6
  [ -e $S/N_syn_${schema}_$kind.done ] && return
  local wu=1; [ $kind = phase1 ] && wu=10
  local out=runs/fresh-nocache/synthetic-$kind.jsonl
  local q=all; [ $kind = phase1 ] && q=$(pick $schema)
  [ -z "$q" ] && return
  echo "$(date +%T) fresh $schema $kind" >> runs/collect_syn.log
  python3 chunks.py synthetic "$q" $n | while read chunk; do
    [ $kind = phase1 ] && grep -q "\"schema\": \"$schema\", \"query\": \"$chunk\"" $out 2>/dev/null && continue  # resume: single-query chunks already in the file are done
    until healthy; do sleep 30; done
    ./restart_flight.sh
    python3 harness.py run --suite synthetic --schema $schema --queries "$chunk" --modes $modes --configs $cfg --reps $nreps --rep-start 1 --warmup $wu \
      --timeout 300 --kubectl $KC --no-features --out $out >> ${out%.jsonl}.log 2>&1
  done
  touch $S/N_syn_${schema}_$kind.done
}
while read schema; do unit $schema default direct,flight configs/default.json 8 3; done < runs/syn_schemas.txt
while read schema; do unit $schema phase1 flight configs/phase1.json 1 2; done < runs/syn_schemas.txt
echo SYN_DONE >> runs/collect_syn.log
