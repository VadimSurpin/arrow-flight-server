#!/bin/bash
# Reruns the Stage B queries that redo.py dropped as cluster-invalid, one query at a time with a driver restart.
cd /home/vadim/arrowflight-server/ArrowFlight/benchmarks/suites
for spec in sorted:q57,q78 shuffled:q36,q72,q74; do
  l=${spec%%:*}
  for q in $(echo ${spec#*:} | tr , ' '); do
    ./restart_driver.sh
    python3 harness.py run --suite tpcds --modes flight --configs configs/phase1.json --reps 1 --warmup 0 --timeout 300 --kubectl ../../tools/.local/bin/kubectl --schema tpcds_sf100_$l --queries $q --no-features --out runs/tpcds-$l-phase1.jsonl >> runs/tpcds-$l-phase1.log 2>&1
  done
done
echo REDO_DONE >> runs/tpcds-shuffled-phase1.log
