#!/bin/bash
# Samples Flight pod CPU and memory once a minute next to the fresh-pod collection (read-only), to see memory growth against the query load.
export KUBECONFIG=/home/vadim/cloudru-k8s/.kube/kubeconfig-surpin.yaml
KC=/home/vadim/arrowflight-server/ArrowFlight/tools/.local/bin/kubectl
while true; do
  $KC -n tpch-bench top pods --no-headers 2>/dev/null | grep '^flight-' | awk -v t="$(date +%T)" '{print t","$1","$2","$3}' >> /home/vadim/arrowflight-server/ArrowFlight/benchmarks/suites/runs/fresh/podmem.csv
  sleep 60
done
