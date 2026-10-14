package pro.surpin.data.arrowflight.server;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import pro.surpin.data.arrowflight.server.adapters.ConfigAdapter;

import static org.junit.jupiter.api.Assertions.assertEquals;

/** Verifies server Arrow allocations respect the configured process budget. */
@Tag("unit")
class ServerModuleAllocatorTest {

    /** Checks that the module passes the configured limit to the root allocator. */
    @Test
    void allocatorUsesConfiguredLimit() {
        ServerModule module = new ServerModule(new String[0], "grpc+tcp://localhost:32010",
                "localhost", new Configuration());
        try (BufferAllocator allocator = module.allocator(ConfigAdapter.getConfig())) {
            assertEquals(2_147_483_648L, allocator.getLimit());
        }
    }
}
