#!/bin/bash
# Re-collection with fresh Flight pods: aged pods measured up to 4x slower, so every chunk starts with a Flight restart.
# Default runs: 3 repetitions in both modes; phase-1 sweep: 2 repetitions, Flight only. Output in runs/fresh/ (the rep-1 data in runs/ stays as the first pass).
# Queries come from the first-pass phase-1 file (default Flight run under 120 s). Resumable through runs/state/F_*.done.
cd /home/vadim/arrowflight-server/ArrowFlight/benchmarks/suites
KC=../../tools/.local/bin/kubectl
export KUBECONFIG=/home/vadim/cloudru-k8s/.kube/kubeconfig-surpin.yaml
K="$KC -n tpch-bench"
S=runs/state
healthy() {
  [ "$($K get nodes --no-headers | grep -c NotReady)" = 0 ] && [ -z "$($K get pods --no-headers | grep Terminating)" ]
}
reps() {  # suite layout kind(phase1|default) modes configs chunk reps
  local suite=$1 l=$2 kind=$3 modes=$4 cfg=$5 n=$6 nreps=$7 schema
  [ -e $S/F_${suite}_${l}_$kind.done ] && return
  case $suite in tpcds|tpch|ssb) schema=${suite}_sf100_$l;; esac
  local out=runs/fresh/$suite-$l-$kind.jsonl
  local q; q=$(python3 runs/pick_queries.py runs/$suite-$l-phase1.jsonl 120)
  [ -z "$q" ] && return
  echo "$(date +%T) fresh $suite $l $kind: $(echo $q | tr ',' '\n' | wc -l) queries" >> runs/collect_fresh.log
  python3 chunks.py $suite "$q" $n | while read chunk; do
    until healthy; do sleep 30; done
    ./restart_flight.sh
    python3 harness.py run --suite $suite --schema $schema --queries "$chunk" --modes $modes --configs $cfg --reps $nreps --rep-start 1 --warmup 0 \
      --timeout 300 --kubectl $KC --no-features --out $out >> ${out%.jsonl}.log 2>&1
  done
  touch $S/F_${suite}_${l}_$kind.done
}
for suite in tpch ssb tpcds; do
  for l in asis sorted shuffled; do
    reps $suite $l default direct,flight configs/default.json 5 3
    reps $suite $l phase1 flight configs/phase1.json 1 2
  done
done
echo FRESH_DONE >> runs/collect_fresh.log
