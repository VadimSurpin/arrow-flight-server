# Trino Arrow Flight connector

Read-only Trino connector for this repository's Arrow Flight SQL server. It is a
thin adapter over `flight-client-core`: connection, authentication, retries and
endpoint streaming come from the shared client.

## Compatibility

The connector is built against Trino SPI 446, the last release compiled for
Java 21. Trino 447 and later require Java 22 or newer. Use the connector with
Trino 446, or rebuild it with a newer `trino.version` after the project moves
past Java 21.

## Build and install

```sh
./mvnw package -pl flight-trino -am
```

The build writes a plugin directory and a zip archive:

```text
flight-trino/target/trino-arrow-flight-1.0.0/arrow-flight/
flight-trino/target/trino-arrow-flight-1.0.0.zip
```

Copy the `arrow-flight` directory into `<trino>/plugin/` on the coordinator and
every worker. Then add a catalog file, for example `etc/catalog/flight.properties`:

```properties
connector.name=arrow-flight
flight.host=flight-server
flight.port=32010
flight.user=alice
flight.password=secret
```

## Catalog properties

| Property | Default | Description |
| --- | --- | --- |
| `flight.host` | required | Flight server host. |
| `flight.port` | required | Flight server port. |
| `flight.user` | none | User for password authentication. |
| `flight.password` | none | Password; set this or `flight.bearer-token`. |
| `flight.bearer-token` | none | Bearer token; set this or `flight.password`. |
| `flight.kerberos.service-principal` | none | Kerberos server principal; mutually exclusive with password and bearer authentication. |
| `flight.tls.enabled` | `false` | Connect with TLS. |
| `flight.tls.verify-server` | `true` | Verify the server certificate. |
| `flight.column-quote` | empty | Identifier quote in generated SQL. |
| `flight.max-retries` | client default | Retries for Flight calls and stream opens. |
| `flight.connect-timeout-ms` | client default | Timeout for Flight calls. |

Unknown `flight.*` properties fail catalog creation.

## Kerberos authentication

Kerberos uses credentials already available to the Trino JVM through Hadoop
UGI. Obtain credentials before starting Trino, for example with `kinit`, or
configure a JAAS login and set `-Djavax.security.auth.useSubjectCredsOnly=false`.
Configure the catalog on the coordinator and every worker:

```properties
connector.name=arrow-flight
flight.host=flight.example.com
flight.port=32010
flight.tls.enabled=true
flight.kerberos.service-principal=flight/flight.example.com@EXAMPLE.COM
```

Do not set `flight.password` or `flight.bearer-token` in this mode. The service
principal hostname must match the Flight host. Every Trino node needs access to
the Kerberos configuration, credential cache or keytab login, KDC, and TLS
trust material. The connector authenticates as the Trino service account; it
does not delegate the identity of the Trino query user.

## How a query runs

1. Metadata comes from Flight SQL `GetDbSchemas` and `GetTables` with table schemas.
2. Trino pushes the selected columns and a filter into the table handle.
3. The coordinator sends one `SELECT` to the server and gets its endpoints.
4. Each endpoint becomes one split, so workers read endpoints in parallel.
5. Workers convert each Arrow batch into a Trino page.

Before split planning, Trino obtains the table row count through a read-only
Flight action. The server reads cached Parquet footer metadata; this request
does not create tickets or reserve node load. File bytes are also transported
in the shared client statistics model, but Trino 446 has no table-level byte
estimate field. Column statistics remain unknown.

Schema, table and column names are exposed in lower case. A name is matched to
the server exactly first, then ignoring case.

## Pushdown

- **Projection.** Only the referenced columns are selected. `count(*)` reads one column.
- **Filters.** Equality, `IN`, ranges and `IS [NOT] NULL` are pushed for boolean,
  integer, double, decimal and date columns. Varchar columns get equality and
  `IN` only. Trino still applies every filter to the returned rows.
- **Not pushed.** `LIMIT`, aggregations, joins, and filters on `real`,
  timestamp or time columns.

## Type mapping

| Arrow | Trino |
| --- | --- |
| Bool | `boolean` |
| Int 8/16/32/64 | `tinyint`/`smallint`/`integer`/`bigint` |
| UInt 8/16/32 | `smallint`/`integer`/`bigint` |
| UInt 64 | `decimal(20,0)` |
| Float32 / Float64 | `real` / `double` |
| Decimal up to precision 38 | `decimal(p,s)` |
| Utf8, LargeUtf8 | `varchar` |
| Binary, LargeBinary, FixedSizeBinary | `varbinary` |
| Date | `date` |
| Time | `time(p)` |
| Timestamp without zone | `timestamp(p)` |
| Timestamp with zone | `timestamp(p) with time zone`, in UTC |

The precision `p` is 0, 3, 6 or 9 for second, milli, micro and nano units.
Columns of other types, such as lists, structs and maps, are hidden.

## Limitations

- The connector is read-only.
- Statistics describe referenced Parquet files before filter and aggregation
  pushdown; column-level statistics are not reported.
