package pro.surpin.data.arrowflight.server;

import org.apache.arrow.flight.auth2.CallHeaderAuthenticator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.IOException;
import java.time.Clock;

/** Authenticates Kerberos tickets and issues signed Flight sessions. */
final class KerberosFlightAuthentication implements FlightAuthenticationProvider {
    private final String principal;
    private final String keytab;
    private final byte[] sessionSecret;

    /**
     * Creates a keytab-backed authentication provider.
     *
     * @param principal server Kerberos principal
     * @param keytab server keytab path
     * @param sessionSecret cluster-shared HMAC key
     */
    KerberosFlightAuthentication(String principal, String keytab, byte[] sessionSecret) {
        this.principal = principal;
        this.keytab = keytab;
        this.sessionSecret = sessionSecret.clone();
    }

    @Override
    public void initialize(Configuration hadoopConfig) throws IOException {
        hadoopConfig.set("hadoop.security.authentication", "kerberos");
        UserGroupInformation.setConfiguration(hadoopConfig);
        UserGroupInformation.loginUserFromKeytab(principal, keytab);
    }

    @Override
    public CallHeaderAuthenticator authenticator() throws IOException {
        return new KerberosFlightAuthenticator(
                KerberosFlightAuthenticator.gssAcceptor(
                        UserGroupInformation.getLoginUser()),
                new KerberosSessionToken(sessionSecret, Clock.systemUTC()));
    }

    @Override
    public boolean requiresTls() {
        return true;
    }

    @Override
    public boolean establishesIdentity() {
        return true;
    }
}
