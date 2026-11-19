#!/bin/bash
# Reruns the queries that redo.py dropped (cluster-invalid runs), driver restarted per chunk of 10.
cd /home/vadim/arrowflight-server/ArrowFlight/benchmarks/suites
H="python3 harness.py run --suite tpcds --reps 1 --warmup 0 --timeout 300 --kubectl ../../tools/.local/bin/kubectl"
for l in asis sorted shuffled; do
  L=$(cat runs/redo-$l.txt); [ -z "$L" ] && continue
  python3 chunks.py tpcds "$L" 10 | while read chunk; do
    ./restart_driver.sh
    $H --schema tpcds_sf100_$l --queries "$chunk" --out runs/tpcds-$l-default.jsonl >> runs/tpcds-$l-default.log 2>&1
  done
done
