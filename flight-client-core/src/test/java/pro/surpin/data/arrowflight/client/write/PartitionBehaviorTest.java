package pro.surpin.data.arrowflight.client.write;

import pro.surpin.data.arrowflight.client.model.Field;
import pro.surpin.data.arrowflight.client.model.FieldType;
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
        Field[] fields = {
                new Field("name", new FieldType(FieldType.IDs.VARCHAR))
        };

        String[] predicates = pb.calculatePredicates(fields);

        assertEquals(2, predicates.length);
        assertTrue(predicates[0].contains("%"));
        assertTrue(predicates[0].contains("hash"));
    }

    @Test
    void calculatePredicatesLongRange() {
        PartitionBehavior pb = new PartitionBehavior("hash", "id", 2, "0", "100", null);
        Field[] fields = {
                new Field("id", new FieldType(FieldType.IDs.LONG))
        };

        String[] predicates = pb.calculatePredicates(fields);

        assertEquals(2, predicates.length);
        assertTrue(predicates[0].contains("id"));
        assertFalse(predicates[0].contains("%"));
    }

    @Test
    void calculatePredicatesDoubleRange() {
        PartitionBehavior pb = new PartitionBehavior("hash", "val", 2, "0.0", "10.0", null);
        Field[] fields = {
                new Field("val", new FieldType(FieldType.IDs.DOUBLE))
        };

        String[] predicates = pb.calculatePredicates(fields);

        assertEquals(2, predicates.length);
        assertTrue(predicates[0].contains("val"));
    }

    @Test
    void calculatePredicatesIntegerRange() {
        PartitionBehavior pb = new PartitionBehavior("hash", "id", 3, "0", "99", null);
        Field[] fields = {
                new Field("id", new FieldType(FieldType.IDs.INT))
        };

        String[] predicates = pb.calculatePredicates(fields);

        assertEquals(3, predicates.length);
    }

    @Test
    void calculatePredicatesDateRange() {
        PartitionBehavior pb = new PartitionBehavior("hash", "dt", 2, "2023-01-01", "2023-01-05", null);
        Field[] fields = {
                new Field("dt", new FieldType(FieldType.IDs.DATE))
        };

        String[] predicates = pb.calculatePredicates(fields);

        assertEquals(2, predicates.length);
        assertTrue(predicates[0].contains("dt"));
        assertFalse(predicates[0].contains("%"));
    }
    @Test
    void calculatePredicatesDateRangeExactBounds() {
        PartitionBehavior pb = new PartitionBehavior("hash", "dt", 2, "2023-01-01", "2023-01-05", null);
        Field[] fields = {new Field("dt", new FieldType(FieldType.IDs.DATE))};
        assertArrayEquals(new String[] {
            "'2023-01-01' <= dt and dt < '2023-01-03'",
            "'2023-01-03' <= dt and dt < '2023-01-05'"
        }, pb.calculatePredicates(fields));
    }

    @Test
    void calculatePredicatesTimestampRangeExactBounds() {
        PartitionBehavior pb = new PartitionBehavior("hash", "ts", 2,
                "2023-01-01 00:00:00", "2023-01-01 02:00:00", null);
        Field[] fields = {new Field("ts", new FieldType(FieldType.IDs.TIMESTAMP))};
        assertArrayEquals(new String[] {
            "'2023-01-01 00:00:00' <= ts and ts < '2023-01-01 01:00:00'",
            "'2023-01-01 01:00:00' <= ts and ts < '2023-01-01 02:00:00'"
        }, pb.calculatePredicates(fields));
    }

    @Test
    void calculatePredicatesSlashDateFormat() {
        PartitionBehavior pb = new PartitionBehavior("hash", "dt", 1, "2023/01/01", "2023/01/02", null);
        Field[] fields = {new Field("dt", new FieldType(FieldType.IDs.DATE))};
        assertArrayEquals(new String[] {"'2023/01/1' <= dt and dt < '2023/01/2'"},
                pb.calculatePredicates(fields));
    }

    @Test
    void calculatePredicatesUnparseableDateFallsBackToHash() {
        PartitionBehavior pb = new PartitionBehavior("hash", "dt", 2, "soon", "later", null);
        Field[] fields = {new Field("dt", new FieldType(FieldType.IDs.DATE))};
        assertArrayEquals(new String[] {"(hash(dt) % 2 + 2) % 2 = 0", "(hash(dt) % 2 + 2) % 2 = 1"},
                pb.calculatePredicates(fields));
    }
}
