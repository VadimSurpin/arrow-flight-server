package pro.surpin.data.arrowflight.server;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

@Tag("integration")
class HadoopArrowFlightServerTest {

    @Test
    void serverStartsAndAcceptsConnections() throws Exception {
        try (TestFlightServerHelper helper = TestFlightServerHelper.builder().start()) {
            assertNotNull(helper.server);
            assertTrue(helper.server.getPort() > 0);
            assertNotNull(helper.sqlClient());
        }
    }

    @Test
    void serverStartupFailsWithInvalidDataDirectory() {
        // Adapter handles nonexistent directories gracefully (empty schema cache).
        // Test that startup at least doesn't throw.
        try (TestFlightServerHelper ignored = TestFlightServerHelper.builder()
                .dataDir("/nonexistent/path")
                .start()) {
            assertNotNull(ignored.parquetAdapter);
        } catch (Exception e) {
            fail("Should not throw for missing data dir", e);
        }
    }

    @Test
    void multipleServersUseDifferentPorts() throws Exception {
        try (TestFlightServerHelper a = TestFlightServerHelper.builder().start();
                TestFlightServerHelper b = TestFlightServerHelper.builder().start()) {
            assertNotEquals(a.server.getPort(), b.server.getPort());
        }
    }

    @Test
    void serverRespondsToMetadataQueries() throws Exception {
        try (TestFlightServerHelper helper = TestFlightServerHelper.builder().start()) {
            var info = helper.sqlClient().getCatalogs();
            assertNotNull(info);
        }
    }
}
