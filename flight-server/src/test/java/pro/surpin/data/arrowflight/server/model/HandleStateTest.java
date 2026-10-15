package pro.surpin.data.arrowflight.server.model;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests content-based value semantics for handle state.
 */
@Tag("unit")
class HandleStateTest {

    /**
     * Verifies equal file-path contents produce equal records and hash codes.
     */
    @Test
    void comparesFilePathContents() {
        HandleState first = new HandleState(
                "SELECT 1", new String[] {"a.parquet", "b.parquet"},
                "grpc://server", 42L, true);
        HandleState second = new HandleState(
                "SELECT 1", new String[] {"a.parquet", "b.parquet"},
                "grpc://server", 42L, true);

        assertEquals(first, second);
        assertEquals(first.hashCode(), second.hashCode());
    }

    /**
     * Verifies different file-path contents produce different records.
     */
    @Test
    void distinguishesFilePathContents() {
        HandleState first = HandleState.forServerFiles(
                "SELECT 1", new String[] {"a.parquet"}, "grpc://server", 42L);
        HandleState second = HandleState.forServerFiles(
                "SELECT 1", new String[] {"b.parquet"}, "grpc://server", 42L);

        assertNotEquals(first, second);
    }

    /**
     * Verifies the record representation includes file-path contents.
     */
    @Test
    void formatsFilePathContents() {
        HandleState state = HandleState.forServerFiles(
                "SELECT 1", new String[] {"a.parquet", "b.parquet"},
                "grpc://server", 42L);

        assertTrue(state.toString().contains("filePaths=[a.parquet, b.parquet]"));
    }
}
