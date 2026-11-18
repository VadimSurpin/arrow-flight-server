# Iceberg reads on HDFS

The probe uses the server image, its `hadoopfs` extension and the `DuckDbAdapter` connection setup. Spark writes a Parquet control file and an Iceberg v2 table to a disposable HDFS instance. The JDBC probe reads those files directly. The run returns a nonzero status if setup, a query, or a comparison fails.

Run from the repository root with Docker and Docker Compose v2. The image build downloads the dependencies. DuckDB downloads its Iceberg extension on the first run.

```bash
bash docker/iceberg-probe/run.sh
```

The probe checks ordered IDs and `COUNT(*)` for each source.

| Source | Expected IDs | Count |
| --- | --- | --- |
| Plain Parquet on HDFS | `1, 2` | 2 |
| Initial Iceberg snapshot | `1, 2` | 2 |
| Snapshot after append | `1, 2, 3` | 3 |
| Snapshot after position delete | `1, 3` | 2 |

Spark verifies that the last commit produced a position delete file. The probe uses the metadata URI and snapshot ID captured after each commit, so the first snapshot is read after both subsequent commits. The output includes DuckDB versions, the HDFS extension path and the SQL under test.

If the Parquet control fails, check the recorded DuckDB version and the `hadoopfs` binary before diagnosing the Iceberg scan. The runtime build currently defaults to `hadoopfs` `1.4.1`, while the server POM selects DuckDB JDBC `1.5.6.0`. To compare another locally built server image, set `FLIGHT_RUNTIME_IMAGE` to its tag when running the same probe.

### Spark-only run

Run the fixture with a Spark and Hadoop image. This mode checks the commits and position delete with Spark and prints the snapshot IDs and metadata paths. The JDBC checks run in the full probe.

```bash
bash docker/iceberg-probe/run.sh --spark-only
```

Remove the disposable containers and network when done.

```bash
docker compose -f docker/iceberg-probe/compose.yml down --volumes
# For the Spark-only run
docker compose -f docker/iceberg-probe/compose.yml -f docker/iceberg-probe/compose.spark.yml down --volumes
```
