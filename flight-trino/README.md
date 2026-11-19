# Trino connector

The connector uses the Flight server, client core, root build, Docker and CI from
`main` unchanged. All Trino-specific sources, tests, builds and launch files live here.
Build and test with Java 21, Maven, Git and Python 3; use Linux/WSL for integration tests.

## Build and verify

Run from the repository root:

```sh
bash flight-trino/scripts/build.sh build
bash flight-trino/scripts/build.sh check
```

`build` creates `flight-trino/target/trino-arrow-flight-1.0.0.zip` and the expanded
`flight-trino/target/trino-arrow-flight-1.0.0/arrow-flight/` plugin directory.
The module-local reactor builds the actual sibling sources from `main`; the root
reactor does not include Trino. `check` runs root compile/Checkstyle/SpotBugs/tests,
connector checks and unit tests, then patched-engine integration with the unchanged
Flight server. `integration` runs only the patched-engine scenarios. Extra Maven
arguments, such as `-o`, may follow the action. The source preparation step needs
network access to fetch the files pinned by `trino-fork/BASE_SHA`.

## Server compatibility

Partial aggregation is disabled by default. On the patched engine enable it for
the query connection with `SET SESSION flight.partial_aggregation_pushdown = true`.

| Operation | Execution with the main server |
| --- | --- |
| COUNT of supported physical scalar columns | Flight partial states, native Trino final |
| Integer MIN/MAX and floating MIN | Flight partial states, native Trino final |
| Integer and floating SUM/AVG | Flight partial states, native Trino final |
| Integer, Boolean and VARCHAR grouping keys | Supported with the preceding contracts |
| Decimal SUM/AVG, decimal extrema and floating MAX | Trino |
| Computed projections, CASE/COALESCE and aggregate masks | Trino |
| CHAR/UUID/IP/interval and composite aggregate contracts | Trino |
| Additional grouping key types | Trino |

Any unsupported aggregate declines the complete partial request. The restored typed
expression renderer, logical type readers and engine bridge remain covered by unit
tests; they do not enable unavailable server extensions. Logical types may be created
by native Trino casts from readable physical columns. The connector does not add
logical metadata, binary catalog support or nested schema reconstruction to the server.
Nanosecond zoned timestamps are rejected to prevent silent precision loss.

Statistics use `arrowflight.table_statistics`, without planning execution tickets.
Filtered scans have unknown estimates and contradictory constraints have zero rows.
Automatic hash partitioning falls back to a single scan retaining every native Flight
endpoint. Explicit disjoint predicates and range bounds remain supported.

## Docker and launch

```sh
bash flight-trino/scripts/build.sh image
FLIGHT_DOCKER_NETWORK=arrowflight_default bash flight-trino/scripts/build.sh run
```

Start the main Flight stack with its own existing commands first. Set
`FLIGHT_DOCKER_NETWORK` to its actual Docker network (including when its project name
or network override differs). This Compose project joins that external network and
only manages the Trino container. It does not rebuild, restart or configure Flight.
`FLIGHT_HOST` defaults to `flight-server-1`, `FLIGHT_PORT` to `32010`, and
`FLIGHT_BEARER_TOKEN` to the existing local-development token. Set the token and
`FLIGHT_TLS_ENABLED` to match the server. `TRINO_HTTP_PORT` defaults to loopback port
`18080`; `TRINO_HEAP` defaults to `1G`. Advanced catalog properties can be supplied
by mounting a properties file and setting `FLIGHT_CATALOG_FILE` to its container path.

The image compiles the changed pinned Trino classes against stock 446 dependencies,
then overlays only those classes and their nested classes into the official runtime's
`trino-main` and `trino-spi` jars. Other engine classes and manifests remain unchanged.
The runtime uses Java 21. This avoids rebuilding unrelated Trino plugins and makes
the deployed patch identical to the classes used in patched-engine integration tests.
The connector plugin uses the unchanged client-core sources and the main Netty BOM;
the embedded-server integration profile aligns gRPC with the main server.

## Query

```sh
python3 -m pip install -r flight-trino/requirements.txt
bash flight-trino/scripts/build.sh query --execute 'SELECT count(*) FROM flight.test_schema.test_table'
bash flight-trino/scripts/build.sh query --partial-aggregation --execute 'SELECT sum(id) FROM flight.test_schema.test_table'
```

The Python client uses `TRINO_HOST`, `TRINO_PORT` and `TRINO_USER`. It enables the
partial property on the same connection when requested. Run its tests with:

```sh
python3 -m unittest discover -s flight-trino/src/test/python
```

Stop only this Trino project with `bash flight-trino/scripts/build.sh down`.
