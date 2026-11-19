package pro.surpin.data.arrowflight.server.adapters;

import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import pro.surpin.data.arrowflight.server.TableProvider.EmptyScan;
import pro.surpin.data.arrowflight.server.TableProvider.IcebergSnapshot;
import pro.surpin.data.arrowflight.server.TableProvider.TableIdentifier;
import pro.surpin.data.arrowflight.server.services.MetadataService;

import java.io.FileNotFoundException;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/** Tests snapshot planning against a filesystem-backed Iceberg catalog. */
@Tag("unit")
class IcebergTableProviderTest {

    /** Verifies that later commits cannot change the schema or source returned by an earlier plan. */
    @Test
    void pinsMetadataAcrossCommits(@TempDir Path warehouse) throws Exception {
        var catalog = new HadoopCatalog(new Configuration(), warehouse.toUri().toString());
        try (var provider = new IcebergTableProvider(catalog, "iceberg", "sales")) {
            var namespace = Namespace.of("sales");
            catalog.createNamespace(namespace);
            var id = org.apache.iceberg.catalog.TableIdentifier.of(namespace, "orders");
            var table = catalog.createTable(id,
                    new org.apache.iceberg.Schema(Types.NestedField.required(1, "id", Types.LongType.get())),
                    PartitionSpec.unpartitioned(), Map.of("format-version", "2"));
            var sqlTable = new TableIdentifier("iceberg", "orders");
            var empty = provider.planScan(sqlTable);
            assertInstanceOf(EmptyScan.class, empty.source());
            assertFalse(empty.tableSchema().findField("id").isNullable());
            assertEquals(new ArrowType.Int(64, true), empty.tableSchema().findField("id").getType());

            table.newAppend().appendFile(DataFiles.builder(table.spec())
                    .withPath(warehouse.resolve("part.parquet").toUri().toString())
                    .withFileSizeInBytes(100).withRecordCount(2).build()).commit();
            var first = provider.planScan(sqlTable);
            var initial = assertInstanceOf(IcebergSnapshot.class, first.source());
            assertEquals(table.currentSnapshot().snapshotId(), initial.snapshotId());

            table.updateSchema().renameColumn("id", "order_id").commit();
            var renamed = provider.planScan(sqlTable);
            var renamedSource = assertInstanceOf(IcebergSnapshot.class, renamed.source());
            assertEquals(initial.snapshotId(), renamedSource.snapshotId());
            assertNotEquals(initial.metadataFile(), renamedSource.metadataFile());
            assertEquals("id", first.tableSchema().getFields().getFirst().getName());
            assertEquals("order_id", renamed.tableSchema().getFields().getFirst().getName());

            table.newAppend().appendFile(DataFiles.builder(table.spec())
                    .withPath(warehouse.resolve("extra.parquet").toUri().toString())
                    .withFileSizeInBytes(50).withRecordCount(1).build()).commit();
            var latest = assertInstanceOf(IcebergSnapshot.class, provider.planScan(sqlTable).source());
            assertNotEquals(initial.snapshotId(), latest.snapshotId());
            assertEquals(initial, first.source());
            assertEquals(List.of("orders"), provider.listTables());
            assertThrows(FileNotFoundException.class,
                    () -> provider.planScan(new TableIdentifier("iceberg", "missing")));
        }
    }

    /** Verifies the Iceberg schema uses its configured catalog and discovery filters. */
    @Test
    void routesDiscoveryBySqlSchema() throws Exception {
        var parquet = mock(ParquetAdapter.class);
        when(parquet.getSchemas(null)).thenReturn(Map.of(
                "parquet", new org.apache.hadoop.fs.Path("/parquet"),
                "ICEBERG", new org.apache.hadoop.fs.Path("/collision")));
        when(parquet.getTables("parquet", "part%")).thenReturn(Map.of(
                "parts", new org.apache.hadoop.fs.Path("/parquet/parts")));
        var iceberg = mock(IcebergTableProvider.class);
        when(iceberg.sqlSchema()).thenReturn("iceberg");
        when(iceberg.listTables()).thenReturn(List.of("customers", "orders"));
        try (var metadata = new MetadataService(parquet, iceberg)) {
            assertEquals(List.of("parquet", "iceberg"), metadata.getSchemas(null));
            assertEquals(List.of("iceberg"), metadata.getSchemas("ICE%"));
            assertEquals(List.of("orders"), metadata.getTables("ICEBERG", "ord_rs"));
            assertEquals(List.of("parts"), metadata.getTables("parquet", "part%"));
            assertFalse(metadata.planIcebergScan("SELECT id FROM parquet.parts").isPresent());
        }
    }

    /** Verifies opt-in configuration limits the warehouse to HDFS. */
    @Test
    void validatesWarehouseConfiguration() {
        assertNull(IcebergTableProvider.fromEnvironment(new Configuration(), Map.of()));
        assertThrows(IllegalArgumentException.class, () -> IcebergTableProvider.fromEnvironment(
                new Configuration(), Map.of("FLIGHT_ICEBERG_WAREHOUSE", "file:///warehouse")));
    }
}
