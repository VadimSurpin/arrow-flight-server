package pro.surpin.data.arrowflight.server;

import java.util.Map;

/** Test policy provider registered through Java ServiceLoader. */
public final class TestAuthorizationProviderFactory implements FlightAuthorizationProviderFactory {
    @Override
    public String name() {
        return "test-policy";
    }

    @Override
    public FlightAuthorization create(Map<String, String> environment) {
        return new FlightAuthorization() {
            @Override
            public boolean canSeeSchema(String principal, String schema) {
                return "test-user".equals(principal) && "public".equals(schema);
            }

            @Override
            public boolean canReadTable(String principal, String schema, String table) {
                return canSeeSchema(principal, schema) && "orders".equals(table);
            }

        };
    }
}
