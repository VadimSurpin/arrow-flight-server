#!/bin/bash
# Brings StarRocks up for harness.py --engine starrocks: scales the FE and the 3 BEs, registers the BEs (Job from deploy/k8s-tpch/40-starrocks.yaml) and
# keeps a port-forward of the FE MySQL port on localhost:19030 (run it with nohup setsid; stops with kill of its pid).
# Do not run it while a Spark collection is running: both share the cluster and the Flight pods.
cd "$(dirname "$0")"
KC=${KC:-../../tools/.local/bin/kubectl}
export KUBECONFIG=${KUBECONFIG:-/home/vadim/cloudru-k8s/.kube/kubeconfig-surpin.yaml}
K="$KC -n tpch-bench"
$K scale sts/starrocks-fe --replicas=1
$K rollout status sts/starrocks-fe --timeout=600s
$K scale sts/starrocks-be --replicas=3
$K rollout status sts/starrocks-be --timeout=900s
$K get job starrocks-register >/dev/null 2>&1 && $K delete job starrocks-register
$K apply -f ../../deploy/k8s-tpch/40-starrocks.yaml
$K wait --for=condition=complete job/starrocks-register --timeout=300s
while true; do $K port-forward pod/starrocks-fe-0 19030:9030; sleep 5; done
