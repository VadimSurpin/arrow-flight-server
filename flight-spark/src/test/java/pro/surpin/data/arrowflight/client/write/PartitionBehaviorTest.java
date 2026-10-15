package pro.surpin.data.arrowflight.client.write;

import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class PartitionBehaviorTest {

    @Test
    void constructorAndGetters() {
        PartitionBehavior pb = new PartitionBehavior("hash", "id", 4, "0", "1000", null);

        assertEquals("id", pb.getByColumn());
        assertNull(pb.getPredicates());
        assertTrue(pb.enabled());
        assertFalse(pb.predicateDefined());
    }

    @Test
    void explicitPredicatesTakePrecedence() {
        String[] predicates = {"id = 1", "id = 2"};
        PartitionBehavior pb = new PartitionBehavior("hash", "id", 4, "0", "1000", predicates);

        assertArrayEquals(predicates, pb.getPredicates());
        assertTrue(pb.enabled());
        assertTrue(pb.predicateDefined());
    }

    @Test
    void enabledFalseWhenNoByColumnAndNoPredicates() {
        PartitionBehavior pb = new PartitionBehavior("hash", null, 4, null, null, null);

        assertFalse(pb.enabled());
    }

    @Test
    void enabledTrueWithPredicatesOnly() {
        PartitionBehavior pb = new PartitionBehavior(null, null, 4, null, null, new String[] {"x > 0"});

        assertTrue(pb.enabled());
    }

    @Test
    void calculatePredicatesHashFallbackWhenNullBounds() {
        PartitionBehavior pb = new PartitionBehavior("hash", "id", 3, null, null, null);

        String[] predicates = pb.calculatePredicates(null);

        assertEquals(3, predicates.length);
    }

    @Test
    void calculatePredicatesHashWhenColumnFieldMissing() {
        PartitionBehavior pb = new PartitionBehavior("hash", "id", 2, "0", "100", null);
        StructField[] fields = {
                StructField.apply("name", DataTypes.StringType, true, null)
        };

        String[] predicates = pb.calculatePredicates(fields);

        assertEquals(2, predicates.length);
        assertTrue(predicates[0].contains("%"));
        assertTrue(predicates[0].contains("hash"));
    }

    @Test
    void calculatePredicatesLongRange() {
        PartitionBehavior pb = new PartitionBehavior("hash", "id", 2, "0", "100", null);
        StructField[] fields = {
                StructField.apply("id", DataTypes.LongType, true, null)
        };

        String[] predicates = pb.calculatePredicates(fields);

        assertEquals(2, predicates.length);
        assertTrue(predicates[0].contains("id"));
        assertFalse(predicates[0].contains("%"));
    }

    @Test
    void calculatePredicatesDoubleRange() {
        PartitionBehavior pb = new PartitionBehavior("hash", "val", 2, "0.0", "10.0", null);
        StructField[] fields = {
                StructField.apply("val", DataTypes.DoubleType, true, null)
        };

        String[] predicates = pb.calculatePredicates(fields);

        assertEquals(2, predicates.length);
        assertTrue(predicates[0].contains("val"));
    }

    @Test
    void calculatePredicatesIntegerRange() {
        PartitionBehavior pb = new PartitionBehavior("hash", "id", 3, "0", "99", null);
        StructField[] fields = {
                StructField.apply("id", DataTypes.IntegerType, true, null)
        };

        String[] predicates = pb.calculatePredicates(fields);

        assertEquals(3, predicates.length);
    }

    @Test
    void calculatePredicatesDateRange() {
        PartitionBehavior pb = new PartitionBehavior("hash", "dt", 2, "2023-01-01", "2023-01-05", null);
        StructField[] fields = {
                StructField.apply("dt", DataTypes.DateType, true, null)
        };

        String[] predicates = pb.calculatePredicates(fields);

        assertEquals(2, predicates.length);
        assertTrue(predicates[0].contains("dt"));
        assertFalse(predicates[0].contains("%"));
    }
}
