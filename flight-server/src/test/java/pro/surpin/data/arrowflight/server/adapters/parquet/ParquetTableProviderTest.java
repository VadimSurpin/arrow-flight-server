package pro.surpin.data.arrowflight.server.adapters.parquet;

import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import pro.surpin.data.arrowflight.server.TableProvider.FileListSource;
import pro.surpin.data.arrowflight.server.TableProvider.TableIdentifier;
import pro.surpin.data.arrowflight.server.TableProvider.TableScanPlan;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

/** Verifies Parquet table planning through the provider port. */
@Tag("unit")
class ParquetTableProviderTest {

    /** Verifies the plan carries the table schema and its relative files. */
    @Test
    void plansSchemaAndFiles() throws Exception {
        Schema schema = new Schema(List.of(new Field("id",
                FieldType.nullable(new ArrowType.Int(64, true)), null)));
        ParquetTableProvider provider = new ParquetTableProvider(
                new ParquetTableProvider.ParquetTableFiles() {
                    @Override
                    public Schema tableSchema(String schemaName, String table) {
                        return schema;
                    }

                    @Override
                    public List<String> tableFiles(String schemaName, String table) {
                        return List.of("test_schema/test_table/a.parquet",
                                "test_schema/test_table/b.parquet");
                    }
                });

        TableScanPlan plan = provider.planScan(
                new TableIdentifier("test_schema", "test_table"));

        assertEquals(schema, plan.tableSchema());
        FileListSource source = assertInstanceOf(FileListSource.class, plan.source());
        assertEquals(2, source.files().size());
        assertEquals("test_schema/test_table/a.parquet", source.files().get(0));
    }
}
