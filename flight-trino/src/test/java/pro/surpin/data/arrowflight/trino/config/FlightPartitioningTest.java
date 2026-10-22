package pro.surpin.data.arrowflight.trino.config;

import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Verifies partition validation and lossless automatic boundaries. */
@Tag("unit")
class FlightPartitioningTest {
    /**
     * Creates a schema for a partition column.
     * @param type column type
     * @return schema
     */
    private static Schema schema(ArrowType type) {
        return new Schema(List.of(new Field("id", FieldType.nullable(type), null)));
    }

    /**
     * Creates range partition settings.
     * @param lower lower bound
     * @param upper upper bound
     * @param count number of partitions
     * @return settings
     */
    private static FlightPartitioning range(String lower, String upper, int count) {
        return FlightPartitioning.fromProperties(Map.of("flight.partition.table", "s.t",
                "flight.partition.by-column", "id", "flight.partition.size", String.valueOf(count),
                "flight.partition.lower-bound", lower, "flight.partition.upper-bound", upper));
    }

    /** Checks exact boundaries above the precision of a double and unbounded edges. */
    @Test
    void partitionsLargeIntegersWithoutLosingRows() {
        FlightPartitioning config = range("9223372036854775800", "9223372036854775806", 3);
        assertEquals(List.of("(id IS NULL OR id < 9223372036854775802)",
                "(id >= 9223372036854775802 AND id < 9223372036854775804)",
                "id >= 9223372036854775804"), config.predicates(schema(new ArrowType.Int(64, true)), ""));
        assertTrue(config.appliesTo("s", "t"));
        assertFalse(config.appliesTo("s", "other"));
    }

    /** Checks narrow ranges do not produce repeated equality predicates. */
    @Test
    void collapsesRepeatedCutsAndHandlesSinglePartition() {
        assertEquals(List.of("(id IS NULL OR id < 0)", "id >= 0"),
                range("0", "1", 6).predicates(schema(new ArrowType.Int(32, true)), ""));
        assertEquals(List.of("true"), range("0", "1", 1).predicates(schema(new ArrowType.Int(32, true)), ""));
    }

    /** Checks ISO temporal ranges without relying on the JVM time zone. */
    @Test
    void partitionsDatesAndTimestamps() {
        assertEquals(List.of("(id IS NULL OR id < DATE '2026-01-02')", "id >= DATE '2026-01-02'"),
                range("2026-01-01", "2026-01-03", 2).predicates(schema(new ArrowType.Date(DateUnit.DAY)), ""));
        assertEquals(List.of("(id IS NULL OR id < TIMESTAMP '2026-01-01 01:00')", "id >= TIMESTAMP '2026-01-01 01:00'"),
                range("2026-01-01 00:00:00", "2026-01-01 02:00:00", 2)
                        .predicates(schema(new ArrowType.Timestamp(TimeUnit.MICROSECOND, null)), ""));
    }

    /** Checks nulls are explicitly assigned only to the first hash partition. */
    @Test
    void partitionsHashesWithQuotedColumn() {
        FlightPartitioning config = FlightPartitioning.fromProperties(Map.of("flight.partition.table", "s.t",
                "flight.partition.by-column", "id", "flight.partition.size", "2"));
        List<String> predicates = config.predicates(schema(ArrowType.Utf8.INSTANCE), "\"");
        assertEquals(2, predicates.size());
        assertTrue(predicates.getFirst().contains("\"id\" IS NULL OR"));
        assertTrue(predicates.getLast().contains("\"id\" IS NOT NULL AND"));
        assertTrue(predicates.getFirst().contains("hash(\"id\")"));
    }

    /** Checks commas in explicit SQL predicates are retained. */
    @Test
    void preservesExplicitPredicates() {
        FlightPartitioning config = FlightPartitioning.fromProperties(Map.of("flight.partition.table", "s.t",
                "flight.partition.predicates", "id IN (1,2); id NOT IN (1,2) OR id IS NULL"));
        assertEquals(List.of("id IN (1,2)", "id NOT IN (1,2) OR id IS NULL"), config.predicates(null, ""));
        assertEquals(FlightPartitioning.NONE, FlightPartitioning.fromProperties(Map.of()));
    }

    /** Checks malformed settings and unsupported bounds fail explicitly. */
    @Test
    void rejectsInvalidSettings() {
        Map<String, String> valid = Map.of("flight.partition.table", "s.t", "flight.partition.by-column", "id");
        for (Map<String, String> change : List.of(Map.of("flight.partition.size", "0"),
                Map.of("flight.partition.size", "10001"), Map.of("flight.partition.table", ""),
                Map.of("flight.partition.predicates", "id=1"), Map.of("flight.partition.lower-bound", "0"),
                Map.of("flight.partition.hash-function", "hash(id)"))) {
            Map<String, String> props = new HashMap<>(valid);
            props.putAll(change);
            assertThrows(IllegalArgumentException.class, () -> FlightPartitioning.fromProperties(props));
        }
        assertThrows(IllegalArgumentException.class, () -> range("1", "0", 2)
                .predicates(schema(new ArrowType.Int(32, true)), ""));
        assertThrows(IllegalArgumentException.class, () -> range("0", "1", 2)
                .predicates(schema(ArrowType.Utf8.INSTANCE), ""));
        assertThrows(IllegalArgumentException.class, () -> range("0", "1", 2)
                .predicates(new Schema(List.of()), ""));
    }
}
