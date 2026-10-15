package pro.surpin.data.arrowflight.server;

import org.apache.arrow.flight.auth2.CallHeaderAuthenticator;

/** Allows unauthenticated calls only in explicit development mode. */
final class InsecureFlightAuthentication implements FlightAuthenticationProvider {
    @Override
    public CallHeaderAuthenticator authenticator() {
        return null;
    }

    @Override
    public boolean requiresTls() {
        return false;
    }

    @Override
    public boolean establishesIdentity() {
        return false;
    }
}
