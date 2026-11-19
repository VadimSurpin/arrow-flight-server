package pro.surpin.data.arrowflight.server.services;

import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import pro.surpin.data.arrowflight.server.TableProvider.EmptyScan;
import pro.surpin.data.arrowflight.server.TableProvider.IcebergSnapshot;
import pro.surpin.data.arrowflight.server.TableProvider.ScanSource;
import pro.surpin.data.arrowflight.server.TableProvider.TableScanPlan;
import pro.surpin.data.arrowflight.server.model.HandleState;

import java.net.URI;
import java.nio.ByteBuffer;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Tests authentication and compatibility for file and table scan tickets. */
@Tag("unit")
class EndpointTicketCodecTest {

    /** Verifies Parquet endpoints keep their legacy format and load accounting. */
    @Test
    void keepsLegacyFileTicketVersion() {
        var codec = new EndpointTicketCodec(new byte[32]);
        var state = HandleState.forServerFiles("SELECT id FROM parquet.orders", new String[]{"orders/a.parquet"},
                "grpc+tcp://worker:32010", 55L);
        byte[] encoded = codec.encode(state);
        assertEquals(0x41465431, ByteBuffer.wrap(encoded).getInt());
        assertEquals(state, codec.decode(encoded));
    }

    /** Verifies new tickets retain their pinned source and source schema. */
    @Test
    void roundsTripAndAuthenticatesTableScans() throws Exception {
        var codec = new EndpointTicketCodec(new byte[32]);
        var schema = new Schema(List.of(Field.nullable("id", new ArrowType.Int(64, true))));
        for (ScanSource source : List.of(new EmptyScan(), new IcebergSnapshot(
                URI.create("hdfs://namenode/warehouse/sales/orders/metadata/v3.metadata.json"), 1234L))) {
            var state = HandleState.forTableScan("SELECT id FROM iceberg.orders", "grpc+tcp://worker:32010",
                    new TableScanPlan(schema, source));
            byte[] ticket = codec.encode(state);
            assertTrue(codec.isEncoded(ticket));
            assertEquals(0x41465432, ByteBuffer.wrap(ticket).getInt());
            HandleState decoded = codec.decode(ticket);
            assertEquals(state, decoded);
            assertEquals(schema, Schema.fromJSON(decoded.tableScan().schemaJson()));
            ticket[ticket.length - 1] ^= 1;
            assertThrows(IllegalArgumentException.class, () -> codec.decode(ticket));
        }
    }
}
