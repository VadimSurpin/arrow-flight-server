# Trino connector

This connector uses the Flight server and shared client from `main` without modifying them.
Build with Java 21 from the repository root. The local reactor includes the unchanged
root project and Trino, so it uses the current shared sources rather than installed snapshots:

```sh
mvn -f flight-trino/reactor/pom.xml -pl :arrow-flight-trino-connector -am compile checkstyle:check spotbugs:check
mvn -f flight-trino/reactor/pom.xml -pl :arrow-flight-trino-connector -am test
```

Statistics use `arrowflight.table_statistics` with schema/table names and no requested columns.
Filtered scans retain unknown estimates; contradictory constraints report zero rows.
Unsupported statistics actions leave estimates unknown and do not allocate scan tickets.
Arrow table schemas are decoded inside the connector using the Arrow IPC API.
Nanosecond timestamps with a time zone are rejected because DuckDB cannot preserve their
advertised precision. Tables the main server cannot expose remain unavailable; the connector
does not reconstruct nested schemas or add binary catalog support to the server.

Partial aggregation remains disabled by default and requires the patched Trino 446 engine.
The unchanged server supports COUNT, integer MIN/MAX, floating MIN, integer and floating SUM/AVG,
and the existing scalar grouping keys. Decimal SUM/AVG, decimal MIN/MAX and floating MAX remain
in Trino. Their previous pushdown required server extensions for exact decimal accumulation,
footer decoding or negated extrema schemas. A request containing any unsupported aggregate
falls back as a whole; ordinary Arrow decimal reads remain available.

Automatic hash partitions fall back to one full scan because the main parser does not
accept `hash(...)`. All native Flight endpoints are retained. Range bounds and explicit
disjoint predicates remain available for additional connector partitions.

The `integration` profile follows the server's gRPC 1.85.0 and inherited Netty BOM.
Run integration tests on Linux/WSL to avoid Windows Hadoop native-library requirements:

```sh
python3 flight-trino/trino-fork/prepare-tests.py
mvn -f flight-trino/reactor/pom.xml -pl :arrow-flight-trino-connector -am -Pintegration,patched-trino test \
  -DexcludedGroups= -Dtest='TrinoPartialAggregation*Test' -Dgroups='unit | integration' \
  -Dsurefire.failIfNoSpecifiedTests=false
```

