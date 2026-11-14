#!/bin/bash
# Reruns the Stage B queries still missing (sorted q78, shuffled q36 q72 q74); waits for a healthy cluster before each query.
cd /home/vadim/arrowflight-server/ArrowFlight/benchmarks/suites
K="../../tools/.local/bin/kubectl --kubeconfig /home/vadim/cloudru-k8s/.kube/kubeconfig-surpin.yaml -n tpch-bench"
healthy() {
  [ "$($K get nodes --no-headers | grep -c NotReady)" = 0 ] && [ "$($K get sts flight -o jsonpath='{.status.readyReplicas}')" = 3 ] && [ -z "$($K get pods --no-headers | grep Terminating)" ]
}
for spec in sorted:q78 shuffled:q36,q72,q74; do
  l=${spec%%:*}
  for q in $(echo ${spec#*:} | tr , ' '); do
    until healthy; do sleep 30; done
    ./restart_driver.sh
    python3 harness.py run --suite tpcds --modes flight --configs configs/phase1.json --reps 1 --warmup 0 --timeout 300 --kubectl ../../tools/.local/bin/kubectl --schema tpcds_sf100_$l --queries $q --no-features --out runs/tpcds-$l-phase1.jsonl >> runs/tpcds-$l-phase1.log 2>&1
  done
done
echo REDO3_DONE >> runs/tpcds-shuffled-phase1.log
