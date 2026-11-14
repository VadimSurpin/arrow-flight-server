package pro.surpin.data.arrowflight.server.adapters;

import org.apache.arrow.flight.FlightEndpoint;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import pro.surpin.data.arrowflight.server.TestFlightServerHelper;

import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Tests the narrow wire form of DECIMAL columns: unscaled integers instead of 128-bit decimals. */
class DecimalNarrowingTest {

    private static DecimalVector decimals(RootAllocator allocator, String name, int precision, int scale,
            BigDecimal... values) {
        DecimalVector vector = new DecimalVector(name, allocator, precision, scale);
        vector.allocateNew(values.length);
        for (int i = 0; i < values.length; i++) {
            if (values[i] == null) {
                vector.setNull(i);
            } else {
                vector.setSafe(i, values[i]);
            }
        }
        vector.setValueCount(values.length);
        return vector;
    }

    @Test
    @Tag("unit")
    void narrowsDecimalsByPrecisionAndKeepsEverythingElse() {
        try (RootAllocator allocator = new RootAllocator();
                DecimalVector price = decimals(allocator, "price", 15, 2,
                        new BigDecimal("12.34"), null, new BigDecimal("-0.05"), new BigDecimal("9999999999999.99"));
                DecimalVector small = decimals(allocator, "small", 7, 3,
                        new BigDecimal("1.250"), new BigDecimal("-4000.001"), null, new BigDecimal("0.000"));
                DecimalVector wide = decimals(allocator, "wide", 30, 4,
                        new BigDecimal("1.0001"), null, null, new BigDecimal("2.0002"));
                VarCharVector name = new VarCharVector("name", allocator)) {
            name.allocateNew(4);
            for (int i = 0; i < 4; i++) {
                name.setSafe(i, ("n" + i).getBytes());
            }
            name.setValueCount(4);
            VectorSchemaRoot source = VectorSchemaRoot.of(price, small, wide, name);

            assertTrue(DecimalNarrowing.applicable(source.getSchema()));
            try (DecimalNarrowing.Narrower narrower = new DecimalNarrowing.Narrower(allocator, source)) {
                narrower.copy();
                VectorSchemaRoot narrow = narrower.root();
                assertEquals(4, narrow.getRowCount());

                Field priceField = narrow.getSchema().findField("price");
                assertEquals(new ArrowType.Int(64, true), priceField.getType());
                assertEquals("15", priceField.getMetadata().get("decimal.precision"));
                assertEquals("2", priceField.getMetadata().get("decimal.scale"));
                assertEquals(new ArrowType.Int(32, true), narrow.getSchema().findField("small").getType());
                assertEquals("3", narrow.getSchema().findField("small").getMetadata().get("decimal.scale"));
                assertTrue(narrow.getSchema().findField("wide").getType() instanceof ArrowType.Decimal);
                assertSame(source.getVector("name"), narrow.getVector("name"));
                assertSame(source.getVector("wide"), narrow.getVector("wide"));

                BigIntVector p = (BigIntVector) narrow.getVector("price");
                assertEquals(1234L, p.get(0));
                assertTrue(p.isNull(1));
                assertEquals(-5L, p.get(2));
                assertEquals(999999999999999L, p.get(3));
                IntVector s = (IntVector) narrow.getVector("small");
                assertEquals(1250, s.get(0));
                assertEquals(-4000001, s.get(1));
                assertTrue(s.isNull(2));
                assertEquals(0, s.get(3));
            }
        }
    }

    @Test
    @Tag("unit")
    void leavesSchemasWithoutNarrowDecimalsAlone() {
        Schema wideOnly = new Schema(List.of(
                new Field("wide", FieldType.nullable(new ArrowType.Decimal(30, 4, 128)), null),
                new Field("id", FieldType.nullable(new ArrowType.Int(64, true)), null)));
        assertFalse(DecimalNarrowing.applicable(wideOnly));
    }

    /** A batch is copied again for every batch of a stream, and the next batch may be shorter. */
    @Test
    @Tag("unit")
    void copiesSuccessiveBatches() {
        try (RootAllocator allocator = new RootAllocator();
                DecimalVector price = decimals(allocator, "price", 10, 2,
                        new BigDecimal("1.00"), new BigDecimal("2.00"), new BigDecimal("3.00"))) {
            VectorSchemaRoot source = VectorSchemaRoot.of(price);
            try (DecimalNarrowing.Narrower narrower = new DecimalNarrowing.Narrower(allocator, source)) {
                narrower.copy();
                assertEquals(3, narrower.root().getRowCount());
                price.setSafe(0, new BigDecimal("7.50"));
                source.setRowCount(1);
                narrower.copy();
                assertEquals(1, narrower.root().getRowCount());
                assertEquals(750L, ((BigIntVector) narrower.root().getVector("price")).get(0));
            }
        }
    }

    private static void writeDecimalTable(Path dir) throws Exception {
        Path table = dir.resolve("test_schema").resolve("decimal_table");
        Files.createDirectories(table);
        try (Connection connection = DriverManager.getConnection("jdbc:duckdb:");
                Statement statement = connection.createStatement()) {
            statement.execute("COPY (SELECT i::BIGINT AS id, "
                    + "CASE WHEN i % 5 = 0 THEN NULL ELSE ((i * 37 % 1000) / 100.0)::DECIMAL(15,2) END AS price, "
                    + "(i / 4.0)::DECIMAL(7,2) AS ratio, i::DECIMAL(30,2) AS big FROM range(1, 2001) t(i)) TO '"
                    + table.resolve("part.parquet") + "' (FORMAT PARQUET)");
        }
    }

    private static List<String> read(TestFlightServerHelper helper, String sql, boolean[] sawNarrow)
            throws Exception {
        FlightInfo info = helper.sqlClient().execute(sql);
        List<String> rows = new ArrayList<>();
        for (FlightEndpoint endpoint : info.getEndpoints()) {
            try (FlightStream stream = helper.sqlClient().getStream(endpoint.getTicket())) {
                while (stream.next()) {
                    VectorSchemaRoot root = stream.getRoot();
                    sawNarrow[0] |= root.getSchema().findField("price").getType() instanceof ArrowType.Int;
                    for (int r = 0; r < root.getRowCount(); r++) {
                        rows.add(root.getVector("id").getObject(r) + "|" + root.getVector("price").getObject(r)
                                + "|" + root.getVector("ratio").getObject(r) + "|" + root.getVector("big").getObject(r));
                    }
                }
            }
        }
        rows.sort(null);
        return rows;
    }

    /** With the option on, the server streams unscaled integers; off, it streams decimals as before. */
    @Test
    @Tag("integration")
    void serverStreamsNarrowDecimalsOnlyWhenEnabled(@TempDir Path dir) throws Exception {
        writeDecimalTable(dir);
        String sql = "SELECT id, price, ratio, big FROM test_schema.decimal_table";
        boolean[] sawNarrow = {false};
        List<String> decimalWire;
        try (TestFlightServerHelper helper = TestFlightServerHelper.builder().dataDir(dir.toString()).start()) {
            decimalWire = read(helper, sql, sawNarrow);
        }
        assertFalse(sawNarrow[0]);
        assertEquals(2000, decimalWire.size());

        System.setProperty("decimalWire", "narrow");
        try (TestFlightServerHelper helper = TestFlightServerHelper.builder().dataDir(dir.toString()).start()) {
            boolean[] narrow = {false};
            FlightInfo info = helper.sqlClient().execute(sql);
            long sum = 0;
            long rows = 0;
            for (FlightEndpoint endpoint : info.getEndpoints()) {
                try (FlightStream stream = helper.sqlClient().getStream(endpoint.getTicket())) {
                    while (stream.next()) {
                        VectorSchemaRoot root = stream.getRoot();
                        narrow[0] |= root.getSchema().findField("price").getType() instanceof ArrowType.Int;
                        assertTrue(root.getSchema().findField("big").getType() instanceof ArrowType.Decimal);
                        BigIntVector price = (BigIntVector) root.getVector("price");
                        for (int r = 0; r < root.getRowCount(); r++) {
                            rows++;
                            if (!price.isNull(r)) {
                                sum += price.get(r);
                            }
                        }
                    }
                }
            }
            assertTrue(narrow[0]);
            assertEquals(2000, rows);
            long expected = 0;
            for (int i = 1; i <= 2000; i++) {
                if (i % 5 != 0) {
                    expected += Math.round(((i * 37L) % 1000) / 100.0 * 100);
                }
            }
            assertEquals(expected, sum);
        } finally {
            System.clearProperty("decimalWire");
        }
    }
}
