#!/bin/bash
# Collects the remaining datasets of the learning set, resumable: a finished stage leaves runs/state/<stage>.done.
#   G*  generate missing layouts (Jobs on compute4, one at a time)
#   restart the Flight pods so they serve the new schemas
#   A_* Stage A: default config, both modes, one run per query (driver restarted per query chunk of 10)
#   B_* Stage B: phase-1 knob sweep, Flight only (driver restarted per query)
#   layout features for all schemas
cd /home/vadim/arrowflight-server/ArrowFlight/benchmarks/suites
KC=../../tools/.local/bin/kubectl
export KUBECONFIG=/home/vadim/cloudru-k8s/.kube/kubeconfig-surpin.yaml
K="$KC -n tpch-bench"
S=runs/state
mkdir -p $S

healthy() {
  [ "$($K get nodes --no-headers | grep -c NotReady)" = 0 ] && [ "$($K get sts flight -o jsonpath='{.status.readyReplicas}')" = 3 ] && [ -z "$($K get pods --no-headers | grep Terminating)" ]
}
wait_healthy() { until healthy; do sleep 30; done; }
stage() { [ -e $S/$1.done ] && return 1; echo "$(date +%T) stage $1" >> runs/collect_all.log; return 0; }
done_() { touch $S/$1.done; }

gen() {  # suite layout
  stage G_$1_$2 || return 0
  python3 bootstrap.py data --suite $1 --sf 100 --layout $2 --out s3://learn --memory 12GB --kubectl $KC --node compute4 > $S/gen.out 2>&1
  job=$(grep -o 'job.batch/[^ ]*' $S/gen.out | head -1 | cut -d/ -f2)
  until [ "$($K get job $job -o jsonpath='{.status.succeeded}')" = 1 ] || [ "$($K get job $job -o jsonpath='{.status.failed}')" = 1 ]; do sleep 60; done
  [ "$($K get job $job -o jsonpath='{.status.succeeded}')" = 1 ] && done_ G_$1_$2 || echo "gen $1 $2 failed" >> runs/collect_all.log
}

run_chunks() {  # suite schema queries chunk out configs modes extra
  local suite=$1 schema=$2 q=$3 n=$4 out=$5 cfg=$6 modes=$7 extra=$8
  python3 chunks.py $suite "$q" $n | while read chunk; do
    wait_healthy
    ./restart_driver.sh
    python3 harness.py run --suite $suite --schema $schema --queries "$chunk" --modes $modes --configs $cfg --reps 1 --warmup 0 --timeout 300 \
      --kubectl $KC $extra --out $out >> ${out%.jsonl}.log 2>&1
  done
}

stageA() {  # suite schema
  stage A_$2 || return 0
  run_chunks $1 $2 all 10 runs/$1-${2##*_}-default.jsonl configs/default.json direct,flight ""
  done_ A_$2
}
stageB() {  # suite schema
  stage B_$2 || return 0
  run_chunks $1 $2 all 1 runs/$1-${2##*_}-phase1.jsonl configs/phase1.json flight --no-features
  done_ B_$2
}

gen tpch sorted; gen tpch shuffled; gen ssb sorted
if stage RESTART_FLIGHT; then
  $K rollout restart sts/flight && $K rollout status sts/flight --timeout=900s && done_ RESTART_FLIGHT
fi
for l in asis sorted shuffled; do stageA tpch tpch_sf100_$l; done
for l in asis sorted shuffled; do stageA ssb ssb_sf100_$l; done
for l in asis sorted shuffled; do stageB tpch tpch_sf100_$l; done
for l in asis sorted shuffled; do stageB ssb ssb_sf100_$l; done
echo ALL_DONE >> runs/collect_all.log
