package pro.surpin.data.arrowflight.server;

import java.util.Map;
import java.util.ServiceLoader;

/** Selects built-in or ServiceLoader security providers from deployment settings. */
final class SecurityProviders {
    private SecurityProviders() {
    }

    /**
     * Selects an authentication provider.
     *
     * @param environment deployment settings
     * @return configured provider
     */
    static FlightAuthenticationProvider authentication(Map<String, String> environment) {
        String name = environment.get("FLIGHT_AUTH_PROVIDER");
        if (name == null || name.isBlank()) {
            name = hasKerberosSettings(environment) ? "kerberos"
                    : environment.get("FLIGHT_BEARER_TOKEN") != null ? "bearer" : "insecure";
        }
        return switch (name) {
            case "kerberos" -> kerberos(environment);
            case "bearer" -> bearer(environment);
            case "insecure" -> insecure(environment);
            default -> customAuthentication(name, environment);
        };
    }

    /**
     * Selects an authorization provider.
     *
     * @param environment deployment settings
     * @return configured provider
     */
    static FlightAuthorization authorization(Map<String, String> environment) {
        String name = authorizationName(environment);
        return switch (name) {
            case "allow-all" -> new AllowAllAuthorization();
            case "ranger" -> RangerAuthorization.fromSettings(environment);
            default -> customAuthorization(name).create(environment);
        };
    }

    /**
     * Reports whether the selected policy requires caller identities.
     *
     * @param environment deployment settings
     * @return true when individual caller identities are required
     */
    static boolean authorizationRequiresIdentity(Map<String, String> environment) {
        String name = authorizationName(environment);
        return switch (name) {
            case "allow-all" -> false;
            case "ranger" -> true;
            default -> customAuthorization(name).requiresIdentity();
        };
    }

    /**
     * Resolves the authorization provider name.
     *
     * @param environment deployment settings
     * @return selected provider name
     */
    private static String authorizationName(Map<String, String> environment) {
        String name = environment.get("FLIGHT_AUTHORIZATION_PROVIDER");
        if (name != null && !name.isBlank()) {
            if (environment.containsKey("RANGER_SERVICE_NAME") && !"ranger".equals(name)) {
                throw new IllegalArgumentException("Ranger settings cannot be combined with "
                        + "another Flight policy provider");
            }
            return name;
        }
        return environment.containsKey("RANGER_SERVICE_NAME") ? "ranger" : "allow-all";
    }

    /**
     * Detects any Kerberos setting to reject partial configurations.
     *
     * @param environment deployment settings
     * @return true when a Kerberos setting is present
     */
    private static boolean hasKerberosSettings(Map<String, String> environment) {
        return environment.containsKey("FLIGHT_KERBEROS_PRINCIPAL")
                || environment.containsKey("FLIGHT_KERBEROS_KEYTAB")
                || environment.containsKey("FLIGHT_KERBEROS_SESSION_SECRET");
    }

    /**
     * Validates bearer deployment settings.
     *
     * @param environment deployment settings
     * @return bearer provider
     */
    private static FlightAuthenticationProvider bearer(Map<String, String> environment) {
        if (hasKerberosSettings(environment)) {
            throw new IllegalArgumentException("Bearer and Kerberos Flight settings cannot be combined");
        }
        return new BearerFlightAuthentication(environment.get("FLIGHT_BEARER_TOKEN"));
    }

    /**
     * Validates explicit insecure development mode.
     *
     * @param environment deployment settings
     * @return unauthenticated provider
     */
    private static FlightAuthenticationProvider insecure(Map<String, String> environment) {
        if (!Boolean.parseBoolean(environment.get("FLIGHT_ALLOW_INSECURE"))
                || hasKerberosSettings(environment)
                || environment.containsKey("FLIGHT_BEARER_TOKEN")) {
            throw new IllegalArgumentException("Insecure Flight mode requires "
                    + "FLIGHT_ALLOW_INSECURE=true and no credentials");
        }
        return new InsecureFlightAuthentication();
    }

    /**
     * Validates keytab and session key settings.
     *
     * @param environment deployment settings
     * @return Kerberos provider
     */
    private static FlightAuthenticationProvider kerberos(Map<String, String> environment) {
        String principal = environment.get("FLIGHT_KERBEROS_PRINCIPAL");
        String keytab = environment.get("FLIGHT_KERBEROS_KEYTAB");
        String secret = environment.get("FLIGHT_KERBEROS_SESSION_SECRET");
        if (environment.containsKey("FLIGHT_BEARER_TOKEN") || principal == null
                || principal.isBlank() || keytab == null || keytab.isBlank()
                || secret == null || secret.isBlank()) {
            throw new IllegalArgumentException("Kerberos Flight requires principal, keytab, "
                    + "session secret, and no static bearer token");
        }
        byte[] decoded = java.util.Base64.getDecoder().decode(secret);
        if (decoded.length < 32) {
            throw new IllegalArgumentException("Kerberos session secret must be at least 32 bytes");
        }
        return new KerberosFlightAuthentication(principal, keytab, decoded);
    }

    /**
     * Loads a named custom authentication provider.
     *
     * @param name configured provider name
     * @param environment deployment settings
     * @return provider
     */
    private static FlightAuthenticationProvider customAuthentication(String name,
            Map<String, String> environment) {
        if (hasKerberosSettings(environment)
                || environment.containsKey("FLIGHT_BEARER_TOKEN")) {
            throw new IllegalArgumentException("Built-in Flight credentials cannot be combined "
                    + "with a custom authentication provider");
        }
        FlightAuthenticationProvider found = null;
        for (FlightAuthenticationProviderFactory factory :
                ServiceLoader.load(FlightAuthenticationProviderFactory.class)) {
            if (name.equals(factory.name())) {
                if (found != null) {
                    throw new IllegalArgumentException("Duplicate Flight auth provider: " + name);
                }
                found = factory.create(environment);
            }
        }
        if (found == null) {
            throw new IllegalArgumentException("Unknown Flight auth provider: " + name);
        }
        return found;
    }

    /**
     * Loads a named custom authorization factory.
     *
     * @param name configured provider name
     * @return provider factory
     */
    private static FlightAuthorizationProviderFactory customAuthorization(String name) {
        FlightAuthorizationProviderFactory found = null;
        for (FlightAuthorizationProviderFactory factory :
                ServiceLoader.load(FlightAuthorizationProviderFactory.class)) {
            if (name.equals(factory.name())) {
                if (found != null) {
                    throw new IllegalArgumentException("Duplicate Flight policy provider: " + name);
                }
                found = factory;
            }
        }
        if (found == null) {
            throw new IllegalArgumentException("Unknown Flight policy provider: " + name);
        }
        return found;
    }
}
