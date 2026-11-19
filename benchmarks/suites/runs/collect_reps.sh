#!/bin/bash
# Noise reduction: two more repetitions (rep 2 and 3) of the Stage B sweep and of the Stage A default runs, for queries that finish
# in under 120 s. Starts when collect_all.sh is done. Resumable through runs/state/R_*.done.
cd /home/vadim/arrowflight-server/ArrowFlight/benchmarks/suites
KC=../../tools/.local/bin/kubectl
export KUBECONFIG=/home/vadim/cloudru-k8s/.kube/kubeconfig-surpin.yaml
K="$KC -n tpch-bench"
S=runs/state
until grep -q ALL_DONE runs/collect_all.log 2>/dev/null; do sleep 120; done
healthy() {
  [ "$($K get nodes --no-headers | grep -c NotReady)" = 0 ] && [ "$($K get sts flight -o jsonpath='{.status.readyReplicas}')" = 3 ] && [ -z "$($K get pods --no-headers | grep Terminating)" ]
}
reps() {  # suite layout kind(phase1|default) modes configs chunk
  local suite=$1 l=$2 kind=$3 modes=$4 cfg=$5 n=$6 schema
  [ -e $S/R_${suite}_${l}_$kind.done ] && return
  case $suite in tpcds|tpch|ssb) schema=${suite}_sf100_$l;; esac
  local out=runs/$suite-$l-$kind.jsonl
  local q; q=$(python3 runs/pick_queries.py runs/$suite-$l-phase1.jsonl 120)
  [ -z "$q" ] && return
  echo "$(date +%T) reps $suite $l $kind: $(echo $q | tr ',' '\n' | wc -l) queries" >> runs/collect_reps.log
  python3 chunks.py $suite "$q" $n | while read chunk; do
    until healthy; do sleep 30; done
    ./restart_driver.sh
    python3 harness.py run --suite $suite --schema $schema --queries "$chunk" --modes $modes --configs $cfg --reps 2 --rep-start 2 --warmup 0 \
      --timeout 300 --kubectl $KC --no-features --out $out >> ${out%.jsonl}.log 2>&1
  done
  touch $S/R_${suite}_${l}_$kind.done
}
for suite in tpch ssb tpcds; do
  for l in asis sorted shuffled; do
    reps $suite $l default direct,flight configs/default.json 5
    reps $suite $l phase1 flight configs/phase1.json 1
  done
done
echo REPS_DONE >> runs/collect_reps.log
