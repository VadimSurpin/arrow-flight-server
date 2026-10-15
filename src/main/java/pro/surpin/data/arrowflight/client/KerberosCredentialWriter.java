package pro.surpin.data.arrowflight.client;

import org.apache.arrow.flight.CallHeaders;
import org.apache.hadoop.security.UserGroupInformation;
import org.ietf.jgss.GSSContext;
import org.ietf.jgss.GSSManager;
import org.ietf.jgss.GSSName;
import org.ietf.jgss.Oid;

import java.util.Base64;
import java.security.PrivilegedExceptionAction;
import java.util.function.Consumer;

/** Produces a SPNEGO token from the client's current Kerberos credentials. */
final class KerberosCredentialWriter implements Consumer<CallHeaders> {
    private static final String SPNEGO_OID = "1.3.6.1.5.5.2";
    private static final String KRB5_PRINCIPAL_OID = "1.2.840.113554.1.2.2.1";
    private final String servicePrincipal;

    /**
     * Creates a writer for the advertised Flight service principal.
     *
     * @param servicePrincipal Kerberos service principal
     */
    KerberosCredentialWriter(String servicePrincipal) {
        this.servicePrincipal = servicePrincipal;
    }

    @Override
    public void accept(CallHeaders headers) {
        try {
            UserGroupInformation user = UserGroupInformation.getCurrentUser();
            byte[] token = user.hasKerberosCredentials()
                    ? user.doAs((PrivilegedExceptionAction<byte[]>) this::createTicket)
                    : createTicket();
            headers.insert("Authorization", "Negotiate " + Base64.getEncoder().encodeToString(token));
        } catch (Exception e) {
            throw new IllegalStateException("Kerberos Flight authentication failed", e);
        }
    }

    /**
     * Creates one SPNEGO service ticket.
     *
     * @return encoded GSS token bytes
     * @throws Exception on GSS failure
     */
    private byte[] createTicket() throws Exception {
            GSSManager manager = GSSManager.getInstance();
            GSSName target = manager.createName(servicePrincipal, new Oid(KRB5_PRINCIPAL_OID));
            GSSContext context = manager.createContext(target, new Oid(SPNEGO_OID), null,
                    GSSContext.DEFAULT_LIFETIME);
            try {
                context.requestMutualAuth(false);
                byte[] token = context.initSecContext(new byte[0], 0, 0);
                if (token == null || token.length == 0) {
                    throw new IllegalStateException("Kerberos did not produce a service ticket");
                }
                return token;
            } finally {
                context.dispose();
            }
    }
}
