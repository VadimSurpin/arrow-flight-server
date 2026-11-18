export MC_CONFIG_DIR=/tmp/mcc
/tmp/mc alias set m http://minio:9000 "$FLIGHT_S3_ACCESS_KEY" "$FLIGHT_S3_SECRET_KEY" >/dev/null
N=${1:-24}
t0=$(date +%s%N)
seq 0 $((N-1)) | xargs -P $N -I{} sh -c '/tmp/mc cat m/learn/tpcds_sf100_asis/store_sales/part-$(printf %03d {}).parquet | wc -c' | awk -v t0=$t0 -v h=$(hostname) '{s+=$1} END {cmd="date +%s%N"; cmd | getline t1; w=(t1-t0)/1e9; printf "%s: %.2f GB in %.2fs = %.2f GB/s\n", h, s/1e9, w, s/1e9/w}'
