package pro.surpin.data.arrowflight.server;

import java.util.Map;

/** Creates a custom Flight authentication provider discovered with ServiceLoader. */
public interface FlightAuthenticationProviderFactory {
    /**
     * Returns the provider name used by FLIGHT_AUTH_PROVIDER.
     *
     * @return unique provider name
     */
    String name();

    /**
     * Builds a provider from deployment settings.
     *
     * @param environment process environment
     * @return authentication provider
     */
    FlightAuthenticationProvider create(Map<String, String> environment);
}
