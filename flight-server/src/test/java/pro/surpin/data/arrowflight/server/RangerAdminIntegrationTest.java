package pro.surpin.data.arrowflight.server;

import org.apache.arrow.flight.FlightRuntimeException;
import org.apache.arrow.flight.FlightStatusCode;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Exercises the embedded Ranger plugin against a live Ranger Admin policy service. */
@Tag("real-services")
class RangerAdminIntegrationTest {
    @Test
    @Timeout(value = 180, unit = TimeUnit.SECONDS)
    void enforcesPoliciesDownloadedFromRangerAdmin() {
        Map<String, String> settings = new HashMap<>();
        settings.put("RANGER_SERVICE_NAME", required("FLIGHT_IT_RANGER_SERVICE"));
        settings.put("RANGER_ADMIN_URL", required("RANGER_ADMIN_URL"));
        copyIfPresent("RANGER_POLICY_REST_USERNAME", settings);
        copyIfPresent("RANGER_POLICY_REST_PASSWORD", settings);
        copyIfPresent("RANGER_POLICY_CACHE_DIR", settings);

        String schema = required("FLIGHT_IT_RANGER_SCHEMA");
        String allowedTable = required("FLIGHT_IT_RANGER_ALLOWED_TABLE");
        String deniedTable = required("FLIGHT_IT_RANGER_DENIED_TABLE");
        String allowedPrincipal = required("FLIGHT_IT_RANGER_ALLOWED_PRINCIPAL");
        String deniedPrincipal = required("FLIGHT_IT_RANGER_DENIED_PRINCIPAL");

        try (RangerAuthorization authorization = RangerAuthorization.fromSettings(settings)) {
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(150);
            boolean allowed = false;
            boolean denied = false;
            while (System.nanoTime() < deadline && !(allowed && denied)) {
                allowed = authorization.canReadTable(allowedPrincipal, schema, allowedTable);
                denied = !authorization.canReadTable(deniedPrincipal, schema, deniedTable);
                if (!(allowed && denied)) {
                    try {
                        Thread.sleep(1_000);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new IllegalStateException("Interrupted waiting for Ranger policies", e);
                    }
                }
            }
            assertTrue(allowed, "Ranger should grant the configured schema and table policy");
            assertTrue(denied, "Ranger should deny the configured restricted table");
            assertFalse(authorization.canSeeSchema(deniedPrincipal,
                    required("FLIGHT_IT_RANGER_HIDDEN_SCHEMA")),
                    "Ranger should hide schemas without a use grant");
            authorization.requireQuery(allowedPrincipal,
                    "SELECT * FROM " + schema + "." + allowedTable);
            assertEquals(FlightStatusCode.UNAUTHORIZED,
                    org.junit.jupiter.api.Assertions.assertThrows(FlightRuntimeException.class,
                            () -> authorization.requireQuery(deniedPrincipal,
                                    "SELECT * FROM " + schema + "." + deniedTable))
                            .status().code());
        }
    }

    private static String required(String name) {
        String value = System.getenv(name);
        if (value == null || value.isBlank()) {
            throw new IllegalStateException("Set " + name + " for the real-services test profile");
        }
        return value;
    }

    private static void copyIfPresent(String name, Map<String, String> target) {
        String value = System.getenv(name);
        if (value != null && !value.isBlank()) {
            target.put(name, value);
        }
    }
}
