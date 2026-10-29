#!/bin/bash
# Restarts the Spark driver and waits until it answers on localhost:18000 (the port-forward loop reconnects by itself).
export KUBECONFIG=${KUBECONFIG:-/home/vadim/cloudru-k8s/.kube/kubeconfig-surpin.yaml}
K=${KUBECTL:-$(dirname "$0")/../../tools/.local/bin/kubectl}
$K -n tpch-bench rollout restart deploy/spark-driver >/dev/null
$K -n tpch-bench rollout status deploy/spark-driver --timeout=600s >/dev/null
for i in $(seq 1 60); do curl -s -m 5 localhost:18000/ | grep -q ok && exit 0; sleep 5; done
exit 1
