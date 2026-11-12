# BenchBase Spark: Comparative Benchmarks on Linux

This guide runs the same TPC-H queries through two paths:

- `Flight`: Spark reads a shared HDFS dataset through Arrow Flight SQL.
- `Direct`: Spark reads the same HDFS dataset through its Parquet DataSource.

The benchmark starts a NameNode and combined storage/compute nodes. Each node
runs an HDFS DataNode, a Flight server and a Spark worker in one container.
HDFS uses `replication=1`; each generated Parquet shard is uploaded from its
colocated DataNode and fits in one HDFS block. Both paths therefore use the
same physical files and consistent hostnames for Spark/Flight data locality.

## Requirements

- Linux x86_64.
- Docker Engine.
- Docker Compose v2 (`docker compose`).
- Bash, Python 3, `awk`, `sed` and `tee`.
- At least 8 GB RAM; 12–16 GB recommended.

Check the environment:

```bash
docker --version
docker compose version
docker info
python3 --version
```

If `docker info` requires root, use `sudo` or add your user to the `docker`
group and start a new login session.

## Full Comparison

From the repository root:

```bash
chmod +x run-benchmark-spark.sh

BENCHMARK_SCALE_FACTOR=0.01 \
BENCHBASE_TIME_SECONDS=60 \
BENCHBASE_WARMUP_SECONDS=30 \
BENCHBASE_TERMINALS=1 \
./run-benchmark-spark.sh \
  tpch compare q1,q6,q14
```

`compare` runs the full cycle:

1. Removes previous benchmark containers and volumes.
2. Starts the HDFS NameNode and configured `DataNode + Flight + Spark worker` nodes.
3. Generates one TPC-H dataset and uploads it to `hdfs://hdfs-namenode:8020/bench`.
4. Publishes `tpch_flight` and `tpch_direct` over the same HDFS Parquet files.
5. Runs the requested number of paired observations, alternating Flight/Direct order.
6. Builds a combined HTML report and updates the local `pages/` directory.

Only `q1`, `q6` and `q14` appear in the generated report and reference results.
BenchBase may print other transaction types with zero counters; those queries
are not executed or added to the per-query report.

To run all 22 TPC-H queries once per path within each paired observation:

```bash
bash benchmarks/benchbase-spark/run-benchbase-spark.sh tpch compare all
```

For results intended for the main curated matrix, run `q1`, `q6` and `q14`
separately, use at least three pairs and record host resources:

```bash
BENCHBASE_PAIRED_OBSERVATIONS=3 \
BENCHBASE_HOST_RESOURCES="8 vCPU, 32 GiB RAM, Spark workers=2" \
BENCHMARK_SCALE_FACTOR=1 \
bash benchmarks/benchbase-spark/run-benchbase-spark.sh tpch compare q1
```

The main page accepts only Q1/Q6/Q14, SF 0.1/1 and topologies with 1/3/8 Flight
nodes. `all`, legacy, smoke and diagnostic runs are published separately on
`exploratory.html`. Lower latency bars indicate faster execution.

The `all` selector enables Q1–Q22 for Flight and Direct. The combined comparison
report and GitHub Pages dashboard can display grouped bars for mean measured
latency per query (`q01`–`q22`), using BenchBase `*.raw.csv` values. The curated
main page retains the eligibility restrictions described above.

For repeated samples of each query, set a measurement duration:

```bash
BENCHBASE_TIME_SECONDS=180 \
BENCHBASE_WARMUP_SECONDS=20 \
bash benchmarks/benchbase-spark/run-benchbase-spark.sh tpch compare all
```

In timed mode, `BENCHBASE_TIME_SECONDS` applies separately to each selected
query. The script creates sequential Q1–Q22 phases, with only one active query
per phase. `BENCHBASE_WARMUP_SECONDS` applies once before the first query of
each engine run. With these values, one engine run takes approximately
`20 + 22 * 180 = 3980` seconds. A default `compare` executes two engine runs
(one pair), approximately 7960 seconds plus data preparation and completion
of in-flight queries. Three publishable pairs take approximately 23880 seconds.

The report is written to:

```text
benchmarks/benchbase-spark/results/tpch-compare-q1,q6,q14-<timestamp>/compare.report.html
```

Each observation and its individual reports are retained:

```text
observations/observation-001/flight/*.report.html
observations/observation-001/direct/*.report.html
observations/observation-002/...
observations/observation-003/...
```

## Main Parameters

```bash
BENCHMARK_SCALE_FACTOR=0.1       # TPC-H scale factor
BENCHBASE_TIME_SECONDS=120      # Measurement seconds per selected query
BENCHBASE_WARMUP_SECONDS=30     # Warmup per engine run
BENCHBASE_TERMINALS=2           # Concurrent BenchBase workers
BENCHBASE_RATE=unlimited        # Request rate limit
BENCHBASE_QUERY_TIMEOUT_SECONDS=120   # BenchBase JDBC query timeout
BENCHBASE_CAPTURE_TIMEOUT_SECONDS=120 # Result-capture query timeout
BENCHBASE_PAIRED_OBSERVATIONS=3       # Opt in to three publishable pairs
BENCHBASE_COMPARE_ORDER=flight-first  # First pair order; subsequent pairs alternate
BENCHBASE_CACHE_POLICY=warm-cache     # One stack without eviction/reset between runs
BENCHBASE_UPDATE_PAGES=false    # Do not update the local pages/ directory
BENCHMARK_OBSERVABILITY=true    # Start Grafana/Prometheus automatically
HDFS_BLOCK_SIZE_BYTES=1073741824 # Each shard must fit in one HDFS block
FLIGHT_BATCH_SIZE=65536         # Arrow/gRPC batch; use 131072 for A/B testing
FLIGHT_DUCKDB_THREADS=2         # Optional override for DuckDB SET threads
FLIGHT_TIMING_LOG_LEVEL=DEBUG   # TIMING events only, with general logging at INFO
```

The Spark benchmark enables `spark.sql.ansi.enabled=true`. Spark DataSource V2
requires ANSI mode for Spark 3.5 to push Q1 decimal expressions such as
`l_extendedprice * (1 - l_discount)` into aggregation. Without that pushdown,
Flight sends millions of input rows back to Spark instead of a few partial
aggregate rows.

Do not enable global `FLIGHT_LOG_LEVEL=DEBUG` when profiling. Synchronous client
and server DEBUG logging distorts the Flight side of the comparison. Use
`FLIGHT_LOG_LEVEL=INFO FLIGHT_TIMING_LOG_LEVEL=DEBUG` to retain structured
`TIMING` records without logging every batch. `FLIGHT_BATCH_SIZE=131072`
reduces the batch count, but must be measured on the target HDFS workload:
the local synthetic scan showed no stable speedup over `65536`.

The two paths within each pair execute sequentially. A single invocation
automatically alternates `Flight → Direct`, then `Direct → Flight`, and so on.
`BENCHBASE_COMPARE_ORDER=direct-first` changes only the first pair's order.
The supported `warm-cache` policy prepares the stack once and does not reset
JVM, Spark, HDFS or OS caches between runs. Alternating order distributes the
order effect. Warmup runs separately at the start of each engine run.

Machine-readable `benchmark-result.json` stores observation values, timestamps,
order, warmup, cache policy, failure metadata and references to all raw CSVs.
Aggregation uses the median of each successful observation as one equally
weighted sample and reports median, min/max spread, p25/p75, IQR and p95.
If an engine fails, the harness still runs the other half of the pair and
subsequent pairs, but excludes that result from the published dashboard.

After measurement, each selected query runs again through `beeline` to capture
its actual output in the HTML report. If capture exceeds
`BENCHBASE_CAPTURE_TIMEOUT_SECONDS`, the script removes the incomplete CSV,
marks the result `not captured` and continues with the next benchmark path.

A larger run:

```bash
BENCHMARK_SCALE_FACTOR=1 \
BENCHBASE_TIME_SECONDS=300 \
BENCHBASE_TERMINALS=4 \
BENCHBASE_UPDATE_PAGES=false \
./run-benchmark-spark.sh \
  tpch compare q1,q6,q14
```

## Single Run

Start Flight from scratch and execute Q6 once:

```bash
./run-benchmark-spark.sh tpch fresh q6
```

Execute several selected queries once each:

```bash
./run-benchmark-spark.sh \
  tpch fresh q1,q6,q14
```

## Grafana and Prometheus

The benchmark script starts the observability stack by default. Disable it
with `BENCHMARK_OBSERVABILITY=false`. To start it manually:

```bash
docker compose --profile observability up -d \
  prometheus grafana node-exporter cadvisor
```

Open `http://<linux-server>:3000/d/arrowflight-benchmark`.
The `ArrowFlight Benchmark` dashboard is provisioned automatically. Prometheus uses port `9090`.
Grafana permits anonymous read-only access, so restrict public access to ports
`3000` and `9090` with a firewall.

If the remote server exposes only SSH, open a separate terminal on your local
computer and create a tunnel:

```bash
ssh -L 13000:127.0.0.1:3000 \
  -p 30105 ssamokhin@84.237.52.100
```

Keep that terminal open. In your local browser, open:

```text
http://localhost:13000/d/arrowflight-benchmark
```

Replace the SSH user, IP and port for another server. Grafana port `3000`
does not need public exposure when using this tunnel.

The dashboard shows:

- Per-core Linux CPU load, host RAM, process and thread counts.
- CPU, working-set RAM and network I/O for each Docker container.
- Heap, non-heap and thread metrics for each Flight JVM.
- Active queries, failures, query rate and p95/mean server execution time.
- Logical Parquet throughput, physical disk throughput and HDFS network I/O.

`Parquet query/read duration` measures the full server path: resolving files,
scanning, filtering/aggregation and sending results. It is a useful benchmark
duration, but does not isolate kernel I/O wait. Use `Linux disk I/O` for physical
read throughput and `Parquet logical throughput` for planned Parquet input size.

Check targets:

```bash
curl -fsS http://localhost:9090/-/ready
curl -fsS http://localhost:9404/metrics  # If a Flight node port is published manually
docker compose --profile observability ps
```

Stop monitoring without deleting history:

```bash
docker compose --profile observability stop \
  grafana prometheus node-exporter cadvisor
```

History is stored in the `grafana-data` and `prometheus-data` volumes, with
15 days of retention by default.

## Diagnostics and Shutdown

View all benchmark service logs:

```bash
./run-benchmark-spark.sh tpch logs
```

Check service status:

```bash
docker compose -f docker-compose.yml --profile benchbase ps
```

Stop the benchmark and remove its volumes:

```bash
./run-benchmark-spark.sh tpch down
```

`compare`, `fresh`, `prepare` and `down` remove benchmark volumes. Do not use
them if the current generated dataset must be preserved.
