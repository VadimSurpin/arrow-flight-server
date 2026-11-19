package pro.surpin.data.arrowflight.server.adapters;

import org.apache.arrow.flight.FlightProducer;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.data.GenericAppenderFactory;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.deletes.PositionDelete;
import org.apache.iceberg.encryption.EncryptedFiles;
import org.apache.iceberg.encryption.EncryptedOutputFile;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import pro.surpin.data.arrowflight.server.TableProvider.IcebergSnapshot;
import pro.surpin.data.arrowflight.server.model.AppConfig;
import pro.surpin.data.arrowflight.server.model.ExecutionPath;
import pro.surpin.data.arrowflight.server.model.ExecutionPathTracker;
import pro.surpin.data.arrowflight.server.model.HandleState;
import pro.surpin.data.arrowflight.server.services.ExecutionService;
import pro.surpin.data.arrowflight.server.services.MetadataService;
import pro.surpin.data.arrowflight.server.services.RuntimeFilters;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/** Tests SQL execution over the Parquet data and deletes of Iceberg snapshots. */
@Tag("unit")
class IcebergReadTest {
    private static final String ALL = "SELECT id FROM iceberg.orders";
    private static final String COUNT = "SELECT COUNT(*) AS total FROM iceberg.orders";

    /** Verifies the same reader handles empty, historical, and updated snapshots. */
    @Test
    void readsOnlyVisibleRowsInPinnedSnapshot(@TempDir Path warehouse) throws Exception {
        var catalog = new org.apache.iceberg.hadoop.HadoopCatalog(new Configuration(), warehouse.toUri().toString());
        var provider = new IcebergTableProvider(catalog, "iceberg", "sales");
        var parquet = mock(ParquetAdapter.class);
        var config = new AppConfig(
                3, 2, 1, 131072, 0, 1, 1,
                null, false, null, null,
                null, null, null, false, 2_147_483_648L, 1048576, 67108864, 60000L, null, null,
                32010, 5701, 60, 3, 1000, 30000);
        try (var metadata = new MetadataService(parquet, provider);
                var allocator = new RootAllocator();
                var pool = Executors.newSingleThreadExecutor();
                var duckdb = new DuckDbAdapter(config, pool)) {
            var runtime = new ExecutionService(parquet, duckdb, metadata, config, pool);
            var namespace = Namespace.of("sales");
            catalog.createNamespace(namespace);
            Table table = catalog.createTable(org.apache.iceberg.catalog.TableIdentifier.of(namespace, "orders"),
                    new org.apache.iceberg.Schema(Types.NestedField.required(1, "id", Types.LongType.get())),
                    PartitionSpec.unpartitioned(), Map.of("format-version", "2"));
            var empty = pin(metadata, COUNT);

            DataFile first = writeRows(table, "first", 1, 2, 3);
            table.newAppend().appendFile(first).commit();
            var original = pin(metadata, ALL);
            table.newAppend().appendFile(writeRows(table, "next", 4)).commit();
            deleteRows(table, first);
            var latest = pin(metadata, ALL);

            assertEquals(List.of(0L), read(allocator, runtime, empty));
            assertEquals(List.of(1L, 2L, 3L), read(allocator, runtime, original));
            assertEquals(List.of(1L, 4L), read(allocator, runtime, latest));
            assertEquals(List.of(2L), read(allocator, runtime, pin(metadata, COUNT)));
            assertEquals(List.of(1L), read(allocator, runtime, pin(metadata, COUNT + " WHERE id > 1")));
            assertEquals(List.of(5.0), read(allocator, runtime, pin(metadata, "SELECT SUM(id) FROM iceberg.orders")));
            String grouped = "SELECT COUNT(*) AS total, id FROM iceberg.orders GROUP BY id";
            assertEquals(List.of(1L, 1L), read(allocator, runtime, pin(metadata, grouped)));
            String aliased = "SELECT id AS order_key FROM iceberg.orders";
            assertEquals(List.of(1L, 4L), read(allocator, runtime, pin(metadata, aliased)));

            table.updateSchema().renameColumn("id", "order_id").commit();
            assertEquals(List.of(4L), read(allocator, runtime,
                    pin(metadata, "SELECT order_id FROM iceberg.orders WHERE order_id > 1")));
            assertEquals(List.of(1L, 4L), read(allocator, runtime, latest));

            var snapshot = (IcebergSnapshot) latest.tableScan().source();
            var mismatch = new HandleState.TableScanState(
                    new IcebergSnapshot(snapshot.metadataFile(), snapshot.snapshotId() ^ 1),
                    latest.tableScan().schemaJson());
            try (var reader = provider.openScan(allocator, mismatch, 2, () -> false)) {
                assertThrows(IOException.class, reader::loadNextBatch);
            }
        }
    }

    /**
     * Pins the table for a query before reading it.
     *
     * @param metadata metadata service
     * @param query SQL query
     * @return ticket state
     * @throws IOException if planning fails
     */
    private static HandleState pin(MetadataService metadata, String query) throws IOException {
        return HandleState.forTableScan(query, null, metadata.planIcebergScan(query).orElseThrow());
    }

    /**
     * Runs the reader and collects its single-column result.
     *
     * @param allocator Arrow allocator
     * @param runtime execution service
     * @param state pinned ticket state
     * @return sorted result values
     * @throws Exception if execution fails
     */
    private static List<Number> read(RootAllocator allocator, ExecutionService runtime, HandleState state)
            throws Exception {
        var listener = mock(FlightProducer.ServerStreamListener.class);
        var root = new AtomicReference<VectorSchemaRoot>();
        List<Number> result = new ArrayList<>();
        when(listener.isReady()).thenReturn(true);
        doAnswer(call -> {
            root.set(call.getArgument(0));
            Schema planned = MetadataService.getQuerySchema(state.query(), Schema.fromJSON(state.tableScan().schemaJson()));
            assertEquals(planned.getFields().stream().map(field -> field.getName()).toList(),
                    root.get().getSchema().getFields().stream().map(field -> field.getName()).toList());
            assertEquals(planned.getFields().stream().map(field -> field.getType()).toList(),
                    root.get().getSchema().getFields().stream().map(field -> field.getType()).toList());
            return null;
        }).when(listener).start(any(VectorSchemaRoot.class));
        doAnswer(call -> {
            var batch = root.get();
            for (int row = 0; row < batch.getRowCount(); row++) {
                result.add((Number) batch.getVector(0).getObject(row));
            }
            return null;
        }).when(listener).putNext();
        var tracker = new ExecutionPathTracker();
        runtime.readIceberg(allocator, state.query(), state.tableScan(), listener, tracker, RuntimeFilters.NONE);
        assertEquals(ExecutionPath.ICEBERG_SCAN, tracker.path());
        result.sort(java.util.Comparator.comparingDouble(Number::doubleValue));
        return result;
    }

    /**
     * Writes Parquet records for an Iceberg append.
     *
     * @param table Iceberg table
     * @param basename file basename
     * @param ids row IDs
     * @return Iceberg data file
     * @throws IOException if writing fails
     */
    private static DataFile writeRows(Table table, String basename, long... ids) throws IOException {
        var writer = new GenericAppenderFactory(table.schema(), table.spec())
                .newDataWriter(output(table, basename), FileFormat.PARQUET, null);
        try (writer) {
            for (long id : ids) {
                writer.write(GenericRecord.create(table.schema()).copy(Map.of("id", id)));
            }
        }
        return writer.toDataFile();
    }

    /**
     * Commits a position delete and an equality delete over existing rows.
     *
     * @param table Iceberg table
     * @param first data file with the test rows
     * @throws IOException if writing fails
     */
    private static void deleteRows(Table table, DataFile first) throws IOException {
        var factory = new GenericAppenderFactory(table.schema(), table.spec(), new int[]{1}, table.schema(), null);
        var position = factory.newPosDeleteWriter(output(table, "position"), FileFormat.PARQUET, null);
        try (position) {
            position.write(PositionDelete.<Record>create().set(first.location(), 1));
        }
        var equality = factory.newEqDeleteWriter(output(table, "equality"), FileFormat.PARQUET, null);
        try (equality) {
            equality.write(GenericRecord.create(table.schema()).copy(Map.of("id", 3L)));
        }
        table.newRowDelta().addDeletes(position.toDeleteFile()).addDeletes(equality.toDeleteFile()).commit();
    }

    /**
     * Resolves an output file inside the table's data directory.
     *
     * @param table Iceberg table
     * @param basename file basename
     * @return unencrypted Parquet output
     */
    private static EncryptedOutputFile output(Table table, String basename) {
        return EncryptedFiles.plainAsEncryptedOutput(
                table.io().newOutputFile(table.location() + "/data/" + basename + ".parquet"));
    }
}
