# Kerberos and Ranger

The Flight server can authenticate clients with Kerberos and enforce Apache Ranger policies on schemas and tables. All Flight nodes must use TLS and share the same session secret. Ranger mode requires Kerberos mode.

## Server setup

Configure every node with:

```sh
FLIGHT_TLS_CERT=/etc/flight/tls.crt
FLIGHT_TLS_KEY=/etc/flight/tls.key
FLIGHT_KERBEROS_PRINCIPAL=flight/node.example.com@EXAMPLE.COM
FLIGHT_KERBEROS_KEYTAB=/etc/flight/flight.keytab
FLIGHT_KERBEROS_SESSION_SECRET=<same base64-encoded random 32-byte-or-longer key on every node>
RANGER_SERVICE_NAME=flight-prod
RANGER_ADMIN_URL=https://ranger.example.com:6182
RANGER_POLICY_CACHE_DIR=/var/lib/flight/ranger
```

Generate the cluster session key with `openssl rand -base64 32` and store it as a secret. Each server principal must match the hostname that clients use for its Kerberos ticket. The nodes exchange short lived, signed bearer sessions after the initial SPNEGO handshake. Clients automatically renew sessions before they expire. Clock synchronization is required across nodes.

Register [ranger-service-definition.json](ranger-service-definition.json) as a Ranger service definition, then create a Ranger service instance named by `RANGER_SERVICE_NAME`. Grant `use` on each visible schema and `select` on each readable table to users or groups. A table read needs both grants. Ranger's policy manager must be reachable at `RANGER_ADMIN_URL`; the server denies access while no service definition or policy result is available. Configure Ranger Admin to permit the Flight plugin to download policies and audit results.

Ranger checks run before statement schema lookup, query planning, and every streamed endpoint ticket. Schema and table metadata lists omit resources without grants. A query that refers to a table without a schema is rejected in Ranger mode. Existing static bearer authentication and explicit insecure development mode remain available when Ranger is disabled.

## Client setup

Obtain Kerberos credentials with a Hadoop UGI or JAAS login before starting the client. A `kinit` ticket cache can be used with `-Djavax.security.auth.useSubjectCredsOnly=false`. In Java, set `Configuration.setKerberosServicePrincipal("flight/node.example.com@EXAMPLE.COM")` on a TLS configuration. In Spark, set `kerberos.servicePrincipal` and `tlsEnabled=true` among the Flight source options. For a certificate signed by a private CA, configure the existing truststore options. The service principal is for the initial coordinator connection; signed sessions are accepted on other Flight nodes.

Ranger evaluates the Kerberos user's short name and the Hadoop groups resolved by the Flight server. Ensure the server's Hadoop group mapping produces the groups used in Ranger policies.
