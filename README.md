# Hadoop Arrow Flight SQL Server

[![CI](https://github.com/VadimSurpin/arrow-flight-server/actions/workflows/ci.yml/badge.svg)](https://github.com/VadimSurpin/arrow-flight-server/actions/workflows/ci.yml)
[![Coveralls](https://coveralls.io/repos/github/VadimSurpin/arrow-flight-server/badge.svg)](https://coveralls.io/github/VadimSurpin/arrow-flight-server)
[![Jacoco Report](https://VadimSurpin.github.io/arrow-flight-server/jacoco/jacoco.svg)](https://VadimSurpin.github.io/arrow-flight-server/jacoco/)
[![Benchmark Pages](https://github.com/VadimSurpin/arrow-flight-server/actions/workflows/benchmark-pages.yml/badge.svg)](https://github.com/VadimSurpin/arrow-flight-server/actions/workflows/benchmark-pages.yml)
[![Benchmark Dashboard](https://img.shields.io/badge/Benchmarks-GitHub_Pages-blue)](https://VadimSurpin.github.io/arrow-flight-server/)
[![License](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](LICENSE)

The public benchmark dashboard opens the curated, schema-valid TPC-H matrix
(Q1, Q6, and Q14; SF 0.1 and 1; 1, 3, and 8 Flight nodes). All-query,
diagnostic, and historical runs are available from its separate exploratory
page and are never mixed into curated charts or conclusions.

High-performance **Arrow Flight SQL** server for analytical queries on Parquet data. Built for teams running SQL over large Parquet datasets in distributed environments (HDFS, S3, local FS).

- **GA**: `SELECT` with `WHERE`, `GROUP BY`, `INNER JOIN`, aggregations (`COUNT`, `SUM`, `MIN`, `MAX`), distributed processing with data locality.
- **Experimental**: Cross-type join auto-coercion (e.g. `INT32 = INT64`, `BOOL = INT8`), Spark DataSource V2 writer path.

---

## Quick Start (Local)

```bash
git clone https://github.com/VadimSurpin/arrow-flight-server.git
cd arrow-flight-server

# Build (Java 21 required, Maven Wrapper included — no Maven install needed)
./mvnw package -DskipTests

# Start a single-node server with test data
java -jar target/hadoop-arrow-flight-1.0-SNAPSHOT.jar \
    --data-dir src/test/resources/test_db \
    --localhost 127.0.0.1 \
    --port 32010 \
    --hosts 127.0.0.1
```

The server is now listening on `grpc://127.0.0.1:32010`. Verify with the Spark client:

```scala
spark.read
  .format("flight")
  .option("host", "127.0.0.1")
  .option("port", "32010")
  .option("table", "SELECT count(*) FROM test_schema.test_table")
  .load()
  .show()
```

Full build and test instructions: **[User Guide — Build and Test](docs/user_guides/build-test-and-scripts.md)**.

---

## Docker Quick Start

A full 8-service cluster orchestrated via `docker-compose.yml` — Spark master, 2 workers, 3 Flight servers, data generator, and a test client.

```bash
# Start the cluster (generates test data automatically)
docker compose up -d

# Run a test query
docker compose --profile test up spark-client
```

| Service | Port | Role |
| :--- | :--- | :--- |
| `flight-server-1` | 32010 | Flight SQL node 1 |
| `flight-server-2` | 32011 → 32010 | Flight SQL node 2 |
| `flight-server-3` | 32012 → 32010 | Flight SQL node 3 |
| `spark-master` | 7077, 8080 | Spark cluster master |
| `spark-worker-1` | 8081 | Spark worker |
| `spark-worker-2` | 8082 | Spark worker |
| `grafana` | 3000 | Benchmark dashboard (`observability` profile) |
| `prometheus` | 9090 | Metrics storage (`observability` profile) |
| `data-generator` | — | Generates and distributes test Parquet files |
| `spark-client` | — | Profiled (`--profile test`), runs `query_flight.py` |

**Dockerfile**: Multi-stage — `maven:3.9.9-eclipse-temurin-21` build and `eclipse-temurin:21-jre-jammy` runtime with Hadoop 3.3.6 and Spark 3.5.9 bundled. DuckDB opens assigned Parquet files and exports results as Arrow batches.

---

## Usage

### Remote Grafana over SSH

Start the observability stack on the Linux server:

```bash
docker compose --profile observability up -d \
  prometheus grafana node-exporter cadvisor
```

If the server exposes only an SSH port, create a tunnel from a terminal on the
local computer and keep that terminal open:

```bash
ssh -L 13000:127.0.0.1:3000 \
  -p 30105 ssamokhin@84.237.52.100
```

Then open the provisioned dashboard in the local browser:

```text
http://localhost:13000/d/arrowflight-benchmark
```

Replace the SSH user, address, and port with values for another server. The
Grafana port does not need to be exposed publicly when the SSH tunnel is used.

### Server Parameters

| Parameter | Description | Default |
| :--- | :--- | :--- |
| `--data-dir` | Directory containing Parquet files | `/data/parquet` |
| `--port` | Flight SQL server port | `32010` |
| `--hosts` | Comma-separated Hazelcast cluster node IPs | `0.0.0.0` |
| `--localhost` | Local IP to bind the server | `localhost` |
| `--hazelcast-port` | Hazelcast communication port | `5701` |

The server waits up to `hazelcastClusterJoinTimeoutSec` (default 60 s) for all `--hosts` nodes to join, then fails with an error. Single-node mode (`--hosts` with one host) skips the wait.

### Spark Connector

Register as the `flight` format:

```scala
val df = spark.read
  .format("flight")
  .option("host", "localhost")
  .option("port", "32010")
  .option("user", "test-user")
  .option("bearerToken", "test-token")
  .option("table", "SELECT * FROM test_schema.test_table")
  .load()
```

Additional options: `tls.*` for TLS, `default.schema`, `routing.tag`/`routing.queue` for workload management, `partition.*` for parallel reads.

### Java Client API

`Client.java` provides a low-level Flight SQL client:

- `fetch(query)` — execute and materialize result
- `fetchStreaming(query, callback)` — stream via `BatchCallback`
- `getQueryEndpoints(query)` — get Flight endpoints
- `execute(sql)` / `executeUpdate(sql)` — arbitrary statements

Supports exponential backoff retry, connection pooling, TLS, BasicAuth and Bearer token authentication.

---

## Benchmark: Arrow Flight vs Direct HDFS Parquet

All 22 TPC-H queries compared across two read paths:

- **Arrow Flight** — Spark reads via `FlightSource`; DuckDB executes the query server-side and streams Arrow IPC batches back over gRPC.
- **Direct** — Spark reads Parquet files from HDFS directly using the native Spark Parquet reader.

### Experiment Setup

| Component | Detail |
| :--- | :--- |
| **Runner** | GitHub Actions `ubuntu-latest` (2 vCPU, 7 GB RAM) |
| **Scale factor** | TPC-H SF=1 (~1 GB) |
| **HDFS cluster** | 1 NameNode + 3 DataNodes (`server-node-{1,2,3}`) |
| **Flight cluster** | 3 Arrow Flight servers, one co-located with each DataNode |
| **Spark cluster** | 1 master + 3 workers (`ci-worker-{1,2,3}`) on **separate** containers |
| **Key design choice** | Spark workers are isolated from DataNodes — direct HDFS reads always traverse the Docker bridge network. Neither path has data locality. This eliminates the bias present in co-located single-node benchmarks. |
| **Pushdown** | Filter, column-projection, and partial-aggregate pushdown into DuckDB (requires `spark.sql.ansi.enabled=true` and the `FlightSessionCatalog` V2 catalog) |
| **HDFS replication** | 1 (data generation: DuckDB → HDFS via Hadoop client) |
| **Repetitions** | 3 timed runs after 1 warmup, per query per engine |
| **Batch size** | 65 536 rows per Arrow IPC batch |

### Results — SF=1 (all 22 queries)

> Speedup = Direct avg / Flight avg; > 1.0 means Arrow Flight is faster. Times are 3-run averages in ms. Sub-second queries carry high relative variance on the 2-vCPU runner.

| Q | Pattern | Flight (ms) | Direct (ms) | Speedup |
|:-:|:--- |--:|--:|:-:|
| Q1 | Full `lineitem` scan + GROUP BY | 460 | 774 | 1.69x 🚀 |
| Q2 | 5-table join + correlated subquery | 1522 | 1443 | 0.95x 🐢 |
| Q3 | 3-table join + top-10 | 2503 | 1802 | 0.72x 🐢 |
| Q4 | `orders` + EXISTS semi-join | 1263 | 1346 | 1.07x 🚀 |
| Q5 | 6-table join, regional revenue | 2933 | 3104 | 1.06x 🚀 |
| Q6 | Selective filter + SUM on `lineitem` | 313 | 318 | 1.02x |
| Q7 | Nation self-join, bilateral shipping | 1914 | 1616 | 0.84x 🐢 |
| Q8 | Nation self-join, market share | 1127 | 2665 | 2.37x 🚀 |
| Q9 | 6-table join, profit by part | 2290 | 7019 | **3.06x** 🚀 |
| Q10 | Returns by customer + nation | 2961 | 3869 | 1.31x 🚀 |
| Q11 | Stock HAVING + correlated subquery | 693 | 549 | 0.79x 🐢 |
| Q12 | Shipmode + date range on `lineitem` | 570 | 4489 | **7.88x** 🚀 |
| Q13 | Customer distribution, LEFT JOIN | 1251 | 1094 | 0.87x 🐢 |
| Q14 | Promo revenue, `lineitem × part` | 195 | 356 | 1.82x 🚀 |
| Q15 | Top supplier (inlined view) | 423 | 693 | 1.64x 🚀 |
| Q16 | Part/supplier COUNT DISTINCT | 340 | 399 | 1.17x 🚀 |
| Q17 | Small-qty revenue, correlated subquery | 1731 | 1542 | 0.89x 🐢 |
| Q18 | Large-volume customer, subquery | 12502 | 10241 | 0.82x 🐢 |
| Q19 | Multi-brand discount revenue | 235 | 463 | 1.97x 🚀 |
| Q20 | Potential promo, nested subquery | 740 | 711 | 0.96x |
| Q21 | EXISTS / NOT EXISTS anti-join | 8171 | 13101 | 1.60x 🚀 |
| Q22 | Phone-code subquery, NOT EXISTS | 1211 | 2957 | 2.44x 🚀 |
| **Score** | | | | **Flight 13 / Tie 2 / Direct 7** |

### Key Findings

**Partial-aggregate + filter pushdown makes Flight the faster path on the majority of TPC-H.** Each Flight server runs the pushed filter and partial aggregate in DuckDB over its own HDFS shard and returns only the reduced result; Spark merges across the 3 servers. Flight wins 13 of 22 queries.

- **Dramatic wins from aggregation/selective filters:** Q12 (7.88×, `l_shipmode IN (…)` + date range collapses to two rows), Q9 (3.06×), Q22 (2.44×), Q8 (2.37×), Q19 (1.97×), Q1 (1.69×) — DuckDB returns far fewer bytes than the raw Parquet blocks Spark would otherwise pull.
- **Direct still wins on some joins:** Q3, Q7, Q13, and correlated-subquery-heavy Q11/Q17 — multi-table joins where Spark's distributed execution across 3 workers, reading raw Parquet, beats routing every table scan through Flight. Because the Flight scan reports raw (pre-pushdown) table sizes, Spark plans these as shuffle-heavy sort-merge joins; reporting post-pushdown sizes so Spark can broadcast the filtered dimensions is in progress on the `perf/pushdown-statistics` branch.
- **Q6 is a wash (1.02×):** its filter is already highly selective, so pushing the aggregate on top adds DuckDB compute without saving meaningful transfer — a case for gating aggregate pushdown on estimated post-filter size.
- **Sub-second queries** (Q6, Q14–Q16, Q19, Q20) carry high relative variance on the constrained runner; treat their ratios as indicative.

---

## Execution Flow

1. **Client** sends SQL via `GetFlightInfo`.
2. **Flight Adapter** parses the query, resolves the cached Arrow schema, and distributes cached file assignments considering locality.
3. Returns `FlightInfo` with endpoints, signed self-contained tickets, and Parquet byte/row estimates.
4. **Client** calls `DoGet` for each endpoint (passing the Ticket).
5. On each node, **Flight Adapter** verifies the ticket and restores its query and file list locally.
6. **Execution Service** uses a Parquet-footer fast path when possible, otherwise executes through DuckDB and streams results as `VectorSchemaRoot`.
7. **Client** receives and processes data.

---

## CI / CD

PR checks (`.github/workflows/ci.yml`) enforce on every pull request:
- `build-server` / `build-client` — compilation via `mvn compile -P server` / `-P client`
- `lint` — Checkstyle violations and SpotBugs errors via `mvn compile checkstyle:check spotbugs:check`
- `integration` — integration + spark + smoke tests via `mvn test -P integration`
- `coverage` — JaCoCo coverage with per-file table in PR comments and detailed HTML report on GitHub Pages

**Run tests locally**:
```bash
./mvnw test                  # unit (excludes integration/spark/perf/smoke)
./mvnw test -P integration   # integration + spark + smoke
./mvnw test -P perf          # performance benchmarks
```

---

## Configuration

Configuration resolves from three tiers: **JVM property** → **`arrowflight.properties`** → **default**.

Key properties (see `AppConfig.java` / `ConfigAdapter.java` for the full list):

| Area | Key Properties |
| :--- | :--- |
| DuckDB | `batchSize`, `duckDbThreads`, `duckDbGroups`, `duckDbWarmConnections` |
| I/O | `ioParallelism`, `ioParallelismMinThreads`, `ioFileBufferSize` |
| Flight/gRPC | `grpcMaxInboundMessageSize`, `flightBackpressureThresholdBytes`, `flightListenerReadyTimeoutMs` |
| Client | `client.maxRetries`, `client.retryBackoffMs`, `client.connectTimeoutMs` |
| Hazelcast | `hazelcastClusterJoinTimeoutSec` |

---

## Features

| Feature | Status |
| :--- | :--- |
| `SELECT` with projection | Supported |
| `WHERE` filtering (server-side via DuckDB) | Supported |
| `COUNT`, `SUM`, `MIN`, `MAX` | Supported |
| `COUNT(DISTINCT col)` | Supported |
| `GROUP BY` | Supported (requires client-side merge) |
| `INNER JOIN` | Supported (DuckDB server-side + Spark fallback) |
| Cross-type join coercion (INT32/INT64, FLOAT/DOUBLE, BOOL/INT8) | Experimental |
| `ORDER BY` | Not supported (server); Supported (Spark client-side) |
| `LIMIT` / `OFFSET` | Not supported (server); Supported (Spark client-side) |
| Subqueries | Experimental |
| Window functions | Not supported |
| Write (`INSERT` / `TRUNCATE`) | Experimental (Spark-side only) |
| Info commands (schemas, tables, types) | Supported |
| Distributed processing with data locality | Supported |

## Limitations

**Server-side (Flight SQL):**
- No `ORDER BY`, `LIMIT`, `OFFSET`, subqueries, or window functions.
- `GROUP BY` results require client-side merging across nodes.

**Spark-side (DataSource V2 connector):**
- `ORDER BY` and `LIMIT` work — Spark applies them after reading from Flight.
- Write support (`INSERT`, `TRUNCATE`) is experimental, limited to Spark DataSource V2 writer path.

---

## Tech Stack

| Technology | Role |
| :--- | :--- |
| **Arrow Flight SQL** | Transport protocol and client API |
| **Apache Spark** | Client-side join execution, data generation |
| **jOOQ** | SQL parsing |
| **Hazelcast** | Distributed cache, node registry, coordination |
| **Hadoop FileSystem** | HDFS / S3 / local FS access |
| **DuckDB** | Parquet scanning, filtering, aggregation and server-side joins |

---

## Documentation

| Document | Description |
| :--- | :--- |
| **[Architecture — Query Execution](docs/architecture/sql-query-execution-flow.md)** | Full query lifecycle: parsing, endpoint routing, footer fast path and DuckDB execution |
| **[Architecture — Parquet Storage](docs/architecture/hadoop-parquet-storage.md)** | Storage model, Hadoop FS abstraction, block locality, file discovery |
| **[ADR](docs/adr/)** | Architecture Decision Records for distribution and benchmark strategy |
| **[User Guide — Build & Test](docs/user_guides/build-test-and-scripts.md)** | Build profiles, unit/integration/perf test commands and `run.sh` usage |
| **[BenchBase Spark — Linux](docs/user_guides/benchbase-spark-linux.ru.md)** | Russian guide for selected TPC-H queries and Flight-vs-Direct comparison runs |
