#!/bin/bash
# Finishes the Flight side of the shuffled layout: q66 (dropped as cluster-invalid) and q69-q99.
cd /home/vadim/arrowflight-server/ArrowFlight/benchmarks/suites
H="python3 harness.py run --suite tpcds --modes flight --reps 1 --warmup 0 --timeout 300 --kubectl ../../tools/.local/bin/kubectl"
python3 chunks.py tpcds "66,69-99" 10 | while read chunk; do
  ./restart_driver.sh
  $H --schema tpcds_sf100_shuffled --queries "$chunk" --out runs/tpcds-shuffled-flight2.jsonl >> runs/tpcds-shuffled-flight2.log 2>&1
done
echo DONE >> runs/tpcds-shuffled-flight2.log
