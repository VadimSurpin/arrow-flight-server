# Build, Tests, and run.sh

This guide explains how to set up the project locally or on a server, how to run tests, and how to use `run.sh`.

## Requirements

You need:

- Linux/WSL or a Linux server
- JDK 21
- Maven
- Git
- Hadoop/Spark environment for HDFS/Spark scenarios

Check the versions:

```bash
java -version
mvn -version
git --version
```

## Get the Branch

From the repository root:

```bash
git fetch origin
git switch feature/goto-duckdb
git pull --ff-only
```

If the local branch does not exist yet:

```bash
git switch --track origin/feature/goto-duckdb
```

Check the current branch:

```bash
git status --short --branch
```

## Build

Fast compilation:

```bash
mvn -q compile
```

Full jar build:

```bash
mvn -q clean package
```

The resulting shaded jar:

```text
target/arrow-flight-server-1.0.0.jar
```

## Unit Tests

By default, integration tests are excluded through the Maven property `excludedGroups=integration`.

Run the regular test suite:

```bash
mvn -q clean test
```

Check the exit code:

```bash
echo $?
```

`0` means the Maven test phase completed successfully.

Important: the logs may contain `ERROR` lines from parser negative tests. For example, some tests intentionally check that `UPDATE ...` or multi-table `FROM a, b` fail with an exception. If Maven exits with code `0`, these log lines are not a test failure.

## Integration Tests

Run integration tests together with the rest of the suite:

```bash
mvn -q -DexcludedGroups="" test
```

Run a specific integration test:

```bash
mvn -q -DexcludedGroups="" -Dtest=ParquetManagerIntegrationTest test
mvn -q -DexcludedGroups="" -Dtest=AggregationIntegrationTest test
```

On Windows, these tests may fail because of Hadoop `winutils` / `HADOOP_HOME`. On a server or in WSL this is usually not an issue.

## Performance Tests

Run the perf benchmark:

```bash
mvn -q -DexcludedGroups="" -Dgroups=perf -Dtest=ArrowFlightPerfTest test
```

Set the data size and number of runs:

```bash
mvn -q -DexcludedGroups="" -Dgroups=perf -Dtest=ArrowFlightPerfTest \
  -Dperf.rows=500000 \
  -Dperf.runs=5 \
  test
```

## Runtime Config

Main config file:

```text
src/main/resources/arrowflight.properties
```

Key parameters:

- `batchSize` - Arrow batch size used by DuckDB export and Flight streaming.
- `ioParallelism` - explicit worker thread count. If empty, the value is calculated.
- `ioParallelismMinThreads` - lower bound for the thread pool.
- `ioParallelismMaxCores` - max CPU cores used in the calculation; `0` means no limit.
- `ioParallelismMultiplier` - core multiplier, `8` by default.
- `duckDbWarmConnections` - number of DuckDB connections warmed up at startup.
- `duckDbGroups` - number of groups used by older grouped helper paths.
- `duckDbThreads` - `SET threads` value for each DuckDB connection.
- `grpcMaxInboundMessageSize` - max inbound gRPC message size.
- `flightBackpressureThresholdBytes` - max serialized outbound bytes queued before Flight applies backpressure.
- `flightListenerReadyTimeoutMs` - timeout for waiting until the Flight listener is ready.

If `ioParallelism` is not set explicitly, the formula is:

```text
max(ioParallelismMinThreads, min(availableProcessors, ioParallelismMaxCores) * ioParallelismMultiplier)
```

JVM system properties override values from the config file:

```bash
mvn test -Darrowflight.io.parallelism=64
mvn test -Darrowflight.duckdb.threads=2
```

## HDFS runtime

HDFS Parquet files are opened by DuckDB through the configured HDFS extension. The Linux runtime provides `libhdfs`, Hadoop configuration, and the Hadoop Java classpath. The project Docker image and entrypoint configure `LD_LIBRARY_PATH`, `HADOOP_CONF_DIR`, and `CLASSPATH` for this purpose.

## run.sh

`run.sh` is a helper for checking a branch on a server.

It can:

- choose a branch
- run `git fetch`
- switch to the branch
- fast-forward the local branch to `origin/<branch>`
- run `mvn compile`
- run the selected tests

By default, `run.sh` is non-destructive. It does not discard local changes and uses `git merge --ff-only`.

If you need to reset the branch exactly to `origin/<branch>`, pass `--force-reset`. When the working tree is dirty, the script prints `git status --short` and requires an explicit typed confirmation before destructive actions.

Examples:

```bash
chmod +x run.sh

./run.sh -b feature/goto-duckdb -t all
./run.sh -b feature/goto-duckdb -t ArrowFlightPerfTest
./run.sh -b feature/goto-duckdb -t perf -r 500000 -n 5
./run.sh -b main -t all --force-reset
```

Modes:

- `-t all` - regular `mvn test`.
- `-t perf` - `ArrowFlightPerfTest` with the `perf` group.
- `-t <ClassName>` - a specific test class.
- `-r ROWS` - `perf.rows`.
- `-n RUNS` - `perf.runs`.
- `--force-reset` - run `git reset --hard origin/<branch>` after explicit dirty-worktree confirmation.
