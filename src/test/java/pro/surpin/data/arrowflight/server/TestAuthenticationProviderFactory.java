package pro.surpin.data.arrowflight.server;

import org.apache.arrow.flight.auth2.CallHeaderAuthenticator;

import java.util.Map;

/** Test provider registered through Java ServiceLoader. */
public final class TestAuthenticationProviderFactory implements FlightAuthenticationProviderFactory {
    @Override
    public String name() {
        return "test-auth";
    }

    @Override
    public FlightAuthenticationProvider create(Map<String, String> environment) {
        return new FlightAuthenticationProvider() {
            @Override
            public CallHeaderAuthenticator authenticator() {
                return headers -> () -> "test-user";
            }

            @Override
            public boolean requiresTls() {
                return true;
            }

            @Override
            public boolean establishesIdentity() {
                return true;
            }
        };
    }
}
