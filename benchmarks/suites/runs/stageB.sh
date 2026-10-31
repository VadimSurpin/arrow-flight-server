#!/bin/bash
# Stage B: one-factor-at-a-time knob changes on the TPC-DS asis queries where Flight took >20 s, failed, or lost to Direct.
cd /home/vadim/arrowflight-server/ArrowFlight/benchmarks/suites
Q="q05,q06,q09,q11,q14,q16,q17,q21,q22,q23,q24,q25,q27,q31,q36,q39,q41,q47,q49,q51,q57,q64,q65,q67,q71,q72,q74,q75,q78,q80,q81,q82,q88,q91,q95,q97"
H="python3 harness.py run --suite tpcds --modes flight --configs configs/phase1.json --reps 1 --warmup 0 --timeout 300 --kubectl ../../tools/.local/bin/kubectl"
python3 chunks.py tpcds "$Q" 1 | while read chunk; do
  ./restart_driver.sh
  $H --schema tpcds_sf100_asis --queries "$chunk" --no-features --out runs/tpcds-asis-phase1.jsonl >> runs/tpcds-asis-phase1.log 2>&1
done
echo DONE >> runs/tpcds-asis-phase1.log
