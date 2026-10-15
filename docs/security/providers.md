# Security providers

The server selects authentication and authorization independently. Existing environment settings still select the built-in providers automatically:

| Authentication | Selection | Authorization | Selection |
| --- | --- | --- | --- |
| Static bearer token | `FLIGHT_BEARER_TOKEN` | Unrestricted access | No policy settings |
| Kerberos | `FLIGHT_KERBEROS_*` | Apache Ranger | `RANGER_SERVICE_NAME` |
| Insecure development | `FLIGHT_ALLOW_INSECURE=true` | | |

Set `FLIGHT_AUTH_PROVIDER` to `bearer`, `kerberos`, `insecure`, or a custom provider name to select authentication explicitly. Set `FLIGHT_AUTHORIZATION_PROVIDER` to `allow-all`, `ranger`, or a custom policy provider name. Insecure mode still requires `FLIGHT_ALLOW_INSECURE=true`; it cannot be combined with credentials. Built-in credential variables cannot be mixed with a custom authentication provider. Ranger settings cannot be combined with another policy provider.

An authorization provider that needs individual caller identities cannot run with the shared bearer token or insecure authentication. Custom authentication providers must return `establishesIdentity() == true` to support such policies. Authentication providers that return `requiresTls() == true` cannot start without `FLIGHT_TLS_CERT` and `FLIGHT_TLS_KEY`.

## Add an authentication provider

Implement `pro.surpin.data.arrowflight.server.FlightAuthenticationProviderFactory` with a unique `name()`. Its `create(Map<String, String>)` method returns a `FlightAuthenticationProvider`. The provider can initialize credentials before Hadoop services start and must return an Arrow `CallHeaderAuthenticator` that supplies an authenticated peer identity for each request. Package the factory class in a JAR with this service descriptor:

```text
META-INF/services/pro.surpin.data.arrowflight.server.FlightAuthenticationProviderFactory
```

The descriptor contains the factory's fully qualified class name. Put the JAR on the server classpath and set `FLIGHT_AUTH_PROVIDER` to its name. The server calls the provider's `close()` method during shutdown.

## Add an authorization provider

Implement `pro.surpin.data.arrowflight.server.FlightAuthorizationProviderFactory`. Its `create(Map<String, String>)` method returns a `FlightAuthorization` implementation that checks schema discovery and table reads. The interface's default query check parses SQL and checks every referenced table, including joins. Register it with:

```text
META-INF/services/pro.surpin.data.arrowflight.server.FlightAuthorizationProviderFactory
```

Set `FLIGHT_AUTHORIZATION_PROVIDER` to its name. The server invokes the policy on statement metadata, planning, and every streamed endpoint ticket. The implementation owns any policy client and releases it in `close()`. New providers should deny when their policy backend is unavailable. The factory's `requiresIdentity()` defaults to true; override it only if the provider intentionally supports shared or anonymous identities.

See [Kerberos and Ranger setup](kerberos-ranger.md) for the built-in deployment settings.
