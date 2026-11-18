#!/bin/bash
# Stage A: default configuration only, TPC-DS queries, both modes, one run per mode (no warm-up), three layouts.
# The Spark driver is restarted every 10 queries: it ran out of heap after about 30 queries in one session.
cd /home/vadim/arrowflight-server/ArrowFlight/benchmarks/suites
export KUBECONFIG=/home/vadim/cloudru-k8s/.kube/kubeconfig-surpin.yaml
K=../../tools/.local/bin/kubectl
H="python3 harness.py run --suite tpcds --reps 1 --warmup 0 --timeout 300 --kubectl $K"

restart_driver() {
  $K -n tpch-bench rollout restart deploy/spark-driver >/dev/null
  $K -n tpch-bench rollout status deploy/spark-driver --timeout=600s >/dev/null
  # the port-forward reconnects by itself; wait until the new driver answers
  for i in $(seq 1 60); do curl -s -m 5 localhost:18000/ | grep -q ok && return; sleep 5; done
}

layer() {  # layout queries
  python3 chunks.py tpcds "$2" 10 | while read chunk; do
    restart_driver
    $H --schema tpcds_sf100_$1 --queries "$chunk" --out runs/tpcds-$1-default.jsonl >> runs/tpcds-$1-default.log 2>&1
  done
}
layer asis "4,8,11,23-99"
layer sorted "1-57,60-99"
layer shuffled "all"
