#!/bin/bash
# Restarts the Flight StatefulSet (fresh pods: aged pods measured up to 4x slower) and waits until all pods are ready, then restarts the Spark driver.
export KUBECONFIG=${KUBECONFIG:-/home/vadim/cloudru-k8s/.kube/kubeconfig-surpin.yaml}
K=${KUBECTL:-$(dirname "$0")/../../tools/.local/bin/kubectl}
$K -n tpch-bench rollout restart sts/flight >/dev/null
$K -n tpch-bench rollout status sts/flight --timeout=900s >/dev/null || exit 1
exec "$(dirname "$0")/restart_driver.sh"
