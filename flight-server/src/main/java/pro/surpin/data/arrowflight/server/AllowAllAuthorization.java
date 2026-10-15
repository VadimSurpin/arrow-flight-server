package pro.surpin.data.arrowflight.server;

/** Preserves unrestricted access when no policy provider is configured. */
public final class AllowAllAuthorization implements FlightAuthorization {
    @Override
    public boolean canSeeSchema(String principal, String schema) {
        return true;
    }

    @Override
    public boolean canReadTable(String principal, String schema, String table) {
        return true;
    }

    @Override
    public void requireQuery(String principal, String query) {
    }
}
