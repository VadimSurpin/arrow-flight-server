#!/bin/bash
# StarRocks direct collection: the same Parquet read straight from S3 through FILES() views (sr_client mode "direct", BE data cache off), for every dataset
# (tpch, ssb, tpcds x asis/sorted/shuffled, clickbench, taxi, 22 synthetic schemas). Default unit only (3 reps, 1 warmup, chunks of 5, 8 for synthetic); the flight.*
# knobs do not apply to a direct read and Flight is not involved, so the Flight pods are not restarted. Waits for SR_DONE of collect_sr.sh, then installs the
# direct-capable harness.py / sr_client.py from runs/srd/ (the running collection re-reads them at every chunk, so they are not replaced earlier).
# Needs the FE port-forward on localhost:19030 (sr_up.sh). Output runs/fresh-sr/<suite>-<layout>-direct.jsonl, markers runs/state/SR_<suite>_<layout>_direct.done, end marker SRD_DONE in runs/collect_srd.log.
cd /home/vadim/arrowflight-server/ArrowFlight/benchmarks/suites
KC=../../tools/.local/bin/kubectl
export KUBECONFIG=/home/vadim/cloudru-k8s/.kube/kubeconfig-surpin.yaml
K="$KC -n tpch-bench"
S=runs/state
until grep -q SR_DONE runs/collect_sr.log; do sleep 300; done
cp runs/srd/sr_client.py sr_client.py; cp runs/srd/harness.py harness.py
export S3_ACCESS_KEY=$($K get secret minio-creds -o jsonpath='{.data.MINIO_ROOT_USER}' | base64 -d)
export S3_SECRET_KEY=$($K get secret minio-creds -o jsonpath='{.data.MINIO_ROOT_PASSWORD}' | base64 -d)
healthy() {
  [ "$($K get nodes --no-headers | grep -c NotReady)" = 0 ] && [ -z "$($K get pods --no-headers | grep Terminating)" ] \
    && [ "$($K get pods --no-headers | grep -E '^starrocks-(fe|be)' | grep -vc ' Running ')" = 0 ] && (exec 3<>/dev/tcp/127.0.0.1/19030) 2>/dev/null
}
unit() {  # suite layout schema chunk
  local suite=$1 l=$2 schema=$3 n=$4
  [ -e $S/SR_${suite}_${l}_direct.done ] && return
  local out=runs/fresh-sr/$suite-$l-direct.jsonl
  echo "$(date +%T) srd $suite $l" >> runs/collect_srd.log
  python3 chunks.py $suite all $n | while read chunk; do
    until healthy; do sleep 30; done
    python3 harness.py run --engine starrocks --suite $suite --schema $schema --queries "$chunk" --modes direct --configs configs/default.json --reps 3 --rep-start 1 --warmup 1 \
      --timeout 300 --kubectl $KC --no-features --out $out >> ${out%.jsonl}.log 2>&1
  done
  touch $S/SR_${suite}_${l}_direct.done
}
for suite in tpch ssb tpcds; do
  for l in asis sorted shuffled; do unit $suite $l ${suite}_sf100_$l 5; done
done
unit clickbench default clickbench 5
unit taxi default taxi 5
while read schema; do unit synthetic $schema $schema 8; done < runs/syn_schemas.txt
echo SRD_DONE >> runs/collect_srd.log
