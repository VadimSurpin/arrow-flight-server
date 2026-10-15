package pro.surpin.data.arrowflight.server;

import org.apache.arrow.flight.CallHeaders;
import org.apache.arrow.flight.CallStatus;
import org.apache.arrow.flight.auth2.CallHeaderAuthenticator;
import org.apache.hadoop.security.UserGroupInformation;
import org.ietf.jgss.GSSContext;
import org.ietf.jgss.GSSCredential;
import org.ietf.jgss.GSSManager;
import org.ietf.jgss.Oid;

import java.security.PrivilegedExceptionAction;
import java.util.Base64;

/** Authenticates a Kerberos Negotiate exchange and signed cluster sessions. */
final class KerberosFlightAuthenticator implements CallHeaderAuthenticator {
    private static final String NEGOTIATE_PREFIX = "Negotiate ";
    private static final String BEARER_PREFIX = "Bearer ";
    private static final String SPNEGO_OID = "1.3.6.1.5.5.2";
    private final TicketAcceptor acceptor;
    private final KerberosSessionToken sessions;

    /** Accepts a GSS service ticket and returns its authenticated principal. */
    @FunctionalInterface
    interface TicketAcceptor {
        /**
         * Validates a GSS service ticket.
         *
         * @param ticket received token bytes
         * @return authenticated client principal
         * @throws Exception on authentication failure
         */
        String accept(byte[] ticket) throws Exception;
    }

    /**
     * Creates a Flight authenticator.
     *
     * @param acceptor Kerberos ticket verifier
     * @param sessions signed bearer session codec
     */
    KerberosFlightAuthenticator(TicketAcceptor acceptor, KerberosSessionToken sessions) {
        this.acceptor = acceptor;
        this.sessions = sessions;
    }

    /**
     * Creates a GSS acceptor bound to the server's keytab login.
     *
     * @param loginUser Hadoop keytab login
     * @return Kerberos ticket verifier
     */
    static TicketAcceptor gssAcceptor(UserGroupInformation loginUser) {
        return ticket -> {
            loginUser.checkTGTAndReloginFromKeytab();
            return loginUser.doAs((PrivilegedExceptionAction<String>) () -> {
                GSSManager manager = GSSManager.getInstance();
                Oid spnego = new Oid(SPNEGO_OID);
                GSSCredential credential = manager.createCredential(null,
                        GSSCredential.INDEFINITE_LIFETIME, spnego, GSSCredential.ACCEPT_ONLY);
                try {
                    GSSContext context = manager.createContext(credential);
                    try {
                        context.acceptSecContext(ticket, 0, ticket.length);
                        if (!context.isEstablished() || context.getSrcName() == null) {
                            throw new IllegalArgumentException("Incomplete Kerberos exchange");
                        }
                        return context.getSrcName().toString();
                    } finally {
                        context.dispose();
                    }
                } finally {
                    credential.dispose();
                }
            });
        };
    }

    @Override
    public AuthResult authenticate(CallHeaders headers) {
        String authorization = headers.get("Authorization");
        if (authorization == null) {
            throw unauthenticated();
        }
        try {
            if (authorization.startsWith(BEARER_PREFIX)) {
                String principal = sessions.verify(authorization.substring(BEARER_PREFIX.length()));
                return () -> principal;
            }
            if (authorization.startsWith(NEGOTIATE_PREFIX)) {
                byte[] ticket = Base64.getDecoder().decode(
                        authorization.substring(NEGOTIATE_PREFIX.length()));
                if (ticket.length == 0 || ticket.length > 65_536) {
                    throw new IllegalArgumentException("Invalid Kerberos token size");
                }
                String principal = acceptor.accept(ticket);
                String token = sessions.issue(principal);
                return new AuthResult() {
                    @Override
                    public String getPeerIdentity() {
                        return principal;
                    }

                    @Override
                    public void appendToOutgoingHeaders(CallHeaders outgoing) {
                        outgoing.insert("Authorization", BEARER_PREFIX + token);
                    }
                };
            }
        } catch (Exception e) {
            throw unauthenticated();
        }
        throw unauthenticated();
    }

    /**
     * Returns a generic authentication error without exposing token details.
     *
     * @return unauthenticated status exception
     */
    private static RuntimeException unauthenticated() {
        return CallStatus.UNAUTHENTICATED
                .withDescription("Invalid Kerberos Flight credential")
                .toRuntimeException();
    }
}
