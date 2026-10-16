package pro.surpin.data.arrowflight.server;

import org.apache.arrow.flight.CallHeaders;
import org.apache.arrow.flight.FlightCallHeaders;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.ietf.jgss.GSSContext;
import org.ietf.jgss.GSSManager;
import org.ietf.jgss.GSSName;
import org.ietf.jgss.Oid;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;
import java.time.Clock;
import java.util.Base64;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Exercises the production GSS acceptor against a provisioned Kerberos KDC. */
@Tag("real-services")
class KerberosKdcIntegrationTest {
    private static final String SPNEGO_OID = "1.3.6.1.5.5.2";

    @Test
    void exchangesRealServiceTicketForSignedFlightSession() throws Exception {
        String krb5Config = required("KRB5_CONFIG");
        System.setProperty("java.security.krb5.conf", krb5Config);
        System.setProperty("javax.security.auth.useSubjectCredsOnly", "true");

        Configuration hadoop = new Configuration();
        hadoop.set("hadoop.security.authentication", "kerberos");
        UserGroupInformation.setConfiguration(hadoop);

        String clientPrincipal = required("FLIGHT_IT_KRB_CLIENT_PRINCIPAL");
        String clientKeytab = required("FLIGHT_IT_KRB_CLIENT_KEYTAB");
        String serverPrincipal = required("FLIGHT_IT_KRB_SERVER_PRINCIPAL");
        String serverKeytab = required("FLIGHT_IT_KRB_SERVER_KEYTAB");
        String host = required("FLIGHT_IT_KRB_SERVICE_HOST");

        UserGroupInformation client = UserGroupInformation.loginUserFromKeytabAndReturnUGI(
                clientPrincipal, clientKeytab);
        UserGroupInformation server = UserGroupInformation.loginUserFromKeytabAndReturnUGI(
                serverPrincipal, serverKeytab);

        byte[] token = client.doAs((java.security.PrivilegedExceptionAction<byte[]>) () -> {
            GSSManager manager = GSSManager.getInstance();
            GSSName service = manager.createName("flight@" + host,
                    GSSName.NT_HOSTBASED_SERVICE);
            Oid spnego = new Oid(SPNEGO_OID);
            GSSContext context = manager.createContext(service, spnego, null,
                    GSSContext.DEFAULT_LIFETIME);
            try {
                return context.initSecContext(new byte[0], 0, 0);
            } finally {
                context.dispose();
            }
        });
        assertTrue(token.length > 0, "KDC should issue a SPNEGO service ticket");

        KerberosFlightAuthenticator authenticator = new KerberosFlightAuthenticator(
                KerberosFlightAuthenticator.gssAcceptor(server),
                new KerberosSessionToken(new byte[32], Clock.systemUTC()));
        FlightCallHeaders request = new FlightCallHeaders();
        request.insert("Authorization", "Negotiate " + Base64.getEncoder().encodeToString(token));
        var authResult = authenticator.authenticate(request);
        assertEquals(clientPrincipal, authResult.getPeerIdentity());

        CallHeaders response = new FlightCallHeaders();
        authResult.appendToOutgoingHeaders(response);
        FlightCallHeaders sessionRequest = new FlightCallHeaders();
        sessionRequest.insert("Authorization", response.get("Authorization"));
        assertEquals(clientPrincipal,
                authenticator.authenticate(sessionRequest).getPeerIdentity());
    }

    private static String required(String name) {
        String value = System.getenv(name);
        if (value == null || value.isBlank()) {
            throw new IllegalStateException("Set " + name + " for the real-services test profile");
        }
        if (name.endsWith("_KEYTAB") || name.equals("KRB5_CONFIG")) {
            Path path = Path.of(value);
            if (!path.toFile().isFile()) {
                throw new IllegalStateException(name + " does not point to a readable file: " + path);
            }
        }
        return value;
    }
}
