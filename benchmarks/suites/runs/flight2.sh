#!/bin/bash
# Flight side of the TPC-DS default run again, after the narrow-decimal SUM fix (Direct results from stage A stay valid).
cd /home/vadim/arrowflight-server/ArrowFlight/benchmarks/suites
H="python3 harness.py run --suite tpcds --modes flight --reps 1 --warmup 0 --timeout 300 --kubectl ../../tools/.local/bin/kubectl"
for l in asis sorted shuffled; do
  python3 chunks.py tpcds all 10 | while read chunk; do
    ./restart_driver.sh
    $H --schema tpcds_sf100_$l --queries "$chunk" --out runs/tpcds-$l-flight2.jsonl >> runs/tpcds-$l-flight2.log 2>&1
  done
done
