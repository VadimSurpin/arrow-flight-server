package pro.surpin.data.arrowflight.server;

import pro.surpin.data.arrowflight.server.adapters.ParquetAdapter;
import pro.surpin.data.arrowflight.server.services.MetadataService;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/** Verifies metadata schema resolution uses the configured table provider. */
@Tag("unit")
class MetadataServiceProviderTest {

    /** Verifies no Parquet adapter schema lookup remains in the metadata path. */
    @Test
    void resolvesTableSchemaThroughProvider() throws Exception {
        ParquetAdapter adapter = mock(ParquetAdapter.class);
        TableProvider provider = mock(TableProvider.class);
        Schema schema = new Schema(List.of());
        TableProvider.TableIdentifier table =
                new TableProvider.TableIdentifier("test_schema", "test_table");
        when(provider.tableSchema(table)).thenReturn(schema);

        MetadataService service = new MetadataService(adapter, provider);

        assertSame(schema, service.getTableSchema("test_schema", "test_table", null));
        verify(provider).tableSchema(table);
        verify(adapter, never()).getTableSchema(anyString(), anyString(), any());
    }
}
