# Real service integration tests

The `real-services` profile runs checks against services provisioned outside the
test JVM. It is separate from the default and ordinary integration profiles
because it requires live StarRocks FE/BE, Flight SQL, Kerberos KDC, and Ranger
Admin instances with test data and policies configured.

```sh
./scripts/run-real-services-tests.sh
```

The suite checks:

- A JDBC query and a live backend through StarRocks FE.
- Catalog, schema, table, schema metadata, and scan planning through the Java
  `flight-starrocks` frontend component against Flight SQL.
- A streamed query through the C++ `FlightScanReader` against Flight SQL.
- A real Kerberos client keytab obtains a SPNEGO service ticket from the KDC;
  the production GSS acceptor validates it and the Flight authenticator issues
  and accepts a signed session token.
- The embedded Ranger plugin downloads policies from Ranger Admin and applies
  schema visibility and table read allow/deny decisions.

The StarRocks test verifies the live FE/BE deployment and the StarRocks module's
client components independently. The repository does not yet contain the
upstream StarRocks FE/BE catalog and scan-node registration, so it cannot claim
to test a native `CREATE EXTERNAL CATALOG` end-to-end flow.

## Required environment

| Variables | Meaning |
| --- | --- |
| `STARROCKS_JDBC_URL`, optional `STARROCKS_USER`, `STARROCKS_PASSWORD` | MySQL protocol URL for a StarRocks FE with at least one live BE. |
| `FLIGHT_IT_HOST`, `FLIGHT_IT_PORT` | Insecure Flight SQL test endpoint serving a catalog, schema, and table. |
| `FLIGHT_IT_CATALOG`, `FLIGHT_IT_SCHEMA`, `FLIGHT_IT_TABLE` | Existing metadata names on that endpoint. |
| `FLIGHT_IT_QUERY` | Query returning at least one row for the C++ streaming test. |
| `KRB5_CONFIG` | Readable Kerberos client configuration file naming the test realm and KDC. |
| `FLIGHT_IT_KRB_CLIENT_PRINCIPAL`, `FLIGHT_IT_KRB_CLIENT_KEYTAB` | Test user's principal and readable keytab. |
| `FLIGHT_IT_KRB_SERVER_PRINCIPAL`, `FLIGHT_IT_KRB_SERVER_KEYTAB` | `flight/host@REALM` principal and readable server keytab. |
| `FLIGHT_IT_KRB_SERVICE_HOST` | Host portion of the `flight/host` SPN used to request the ticket. |
| `RANGER_ADMIN_URL`, `FLIGHT_IT_RANGER_SERVICE` | Live Ranger Admin URL and pre-created `arrowflight` service instance name. |
| `RANGER_POLICY_REST_USERNAME`, `RANGER_POLICY_REST_PASSWORD` | Optional credentials for the Ranger plugin's policy REST requests. |
| `RANGER_POLICY_CACHE_DIR` | Optional writable policy cache directory. |
| `FLIGHT_IT_RANGER_SCHEMA`, `FLIGHT_IT_RANGER_ALLOWED_TABLE`, `FLIGHT_IT_RANGER_DENIED_TABLE` | Resources present in the service's Ranger policies. |
| `FLIGHT_IT_RANGER_ALLOWED_PRINCIPAL`, `FLIGHT_IT_RANGER_DENIED_PRINCIPAL` | Kerberos-style users used for policy allow and deny checks. |
| `FLIGHT_IT_RANGER_HIDDEN_SCHEMA` | Schema for which no `use` policy is granted. |

Register [`ranger-service-definition.json`](../security/ranger-service-definition.json)
and create the named Ranger service before running the suite. Grant `use` on
`FLIGHT_IT_RANGER_SCHEMA` and `select` on
`FLIGHT_IT_RANGER_ALLOWED_TABLE` to the allowed principal. Do not grant the
denied principal access to `FLIGHT_IT_RANGER_DENIED_TABLE`, and do not grant
`use` on the hidden schema. Ranger's policy REST service must permit the plugin
to download policies. The test waits up to 150 seconds for the plugin's first
policy refresh.

The test command intentionally fails when required settings are missing. For
ordinary unit and integration tests, keep using `./mvnw test` and
`./mvnw -P integration test`.
