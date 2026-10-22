package pro.surpin.data.arrowflight.trino.read;

import pro.surpin.data.arrowflight.trino.client.FlightService;
import pro.surpin.data.arrowflight.trino.metadata.FlightColumnHandle;
import pro.surpin.data.arrowflight.trino.metadata.FlightTableHandle;
import pro.surpin.data.arrowflight.trino.metadata.FlightTransactionHandle;

import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorSplitSource;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.BigintType;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import pro.surpin.data.arrowflight.client.query.Endpoint;
import pro.surpin.data.arrowflight.client.query.QueryEndpoints;

import java.net.URI;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@Tag("unit")
class FlightSplitManagerTest {
    private static final FlightTableHandle TABLE =
            FlightTableHandle.of(new SchemaTableName("s", "t"), "s", "t")
                    .withColumns(List.of(new FlightColumnHandle("id", "id", BigintType.BIGINT)));

    private static List<ConnectorSplit> drain(ConnectorSplitSource source) throws Exception {
        List<ConnectorSplit> splits = source.getNextBatch(100).get().getSplits();
        assertTrue(source.isFinished());
        return splits;
    }

    @Test
    void createsOneSplitPerEndpoint() throws Exception {
        FlightService service = mock(FlightService.class);
        Endpoint first = new Endpoint(new URI[] {URI.create("grpc+tcp://node1:32010")}, new byte[] {1});
        Endpoint second = new Endpoint(new URI[0], new byte[] {2, 3});
        when(service.plan("select id from s.t")).thenReturn(new QueryEndpoints(null, new Endpoint[] {first, second}));

        List<ConnectorSplit> splits = drain(new FlightSplitManager(service, "")
                .getSplits(FlightTransactionHandle.INSTANCE, null, TABLE, DynamicFilter.EMPTY, Constraint.alwaysTrue()));

        assertEquals(List.of(new FlightSplit(List.of("grpc+tcp://node1:32010"), new byte[] {1}),
                new FlightSplit(List.of(), new byte[] {2, 3})), splits);
    }

    /**
     * Checks every partition endpoint gets its own split and retains the scan filter.
     * @throws Exception if split retrieval fails
     */
    @Test
    void plansEveryPartitionAndEndpoint() throws Exception {
        FlightService service = mock(FlightService.class);
        var config = pro.surpin.data.arrowflight.trino.config.FlightPartitioning.fromProperties(java.util.Map.of(
                "flight.partition.table", "s.t", "flight.partition.predicates", "id < 10;id >= 10 OR id IS NULL"));
        when(service.listTables("s")).thenReturn(java.util.Map.of("t", new org.apache.arrow.vector.types.pojo.Schema(List.of())));
        when(service.plan(anyString())).thenReturn(new QueryEndpoints(null, new Endpoint[] {
                new Endpoint(new URI[] {URI.create("grpc+tcp://node1:32010")}, new byte[] {1}),
                new Endpoint(new URI[] {URI.create("grpc+tcp://node2:32010")}, new byte[] {2})}));
        FlightColumnHandle id = new FlightColumnHandle("id", "id", BigintType.BIGINT);
        var handle = TABLE.withConstraint(TupleDomain.withColumnDomains(java.util.Map.of(id,
                io.trino.spi.predicate.Domain.singleValue(BigintType.BIGINT, 5L))));
        var manager = new FlightSplitManager(service, "", config);
        List<ConnectorSplit> splits = drain(manager.getSplits(FlightTransactionHandle.INSTANCE, null, handle,
                DynamicFilter.EMPTY, Constraint.alwaysTrue()));
        assertEquals(4, splits.size());
        verify(service).plan("select id from s.t where (id = 5) AND (id < 10)");
        verify(service).plan("select id from s.t where (id = 5) AND (id >= 10 OR id IS NULL)");
    }

    /**
     * Checks settings for one table do not partition unrelated tables.
     * @throws Exception if split retrieval fails
     */
    @Test
    void leavesOtherTablesUnpartitioned() throws Exception {
        FlightService service = mock(FlightService.class);
        var config = pro.surpin.data.arrowflight.trino.config.FlightPartitioning.fromProperties(java.util.Map.of(
                "flight.partition.table", "s.other", "flight.partition.by-column", "missing"));
        when(service.plan("select id from s.t")).thenReturn(new QueryEndpoints(null, new Endpoint[0]));
        drain(new FlightSplitManager(service, "", config).getSplits(FlightTransactionHandle.INSTANCE, null,
                TABLE, DynamicFilter.EMPTY, Constraint.alwaysTrue()));
        verify(service, never()).listTables(anyString());
        verify(service).plan("select id from s.t");
    }

    @Test
    void skipsPlanningForEmptyFilter() throws Exception {
        FlightService service = mock(FlightService.class);
        List<ConnectorSplit> splits = drain(new FlightSplitManager(service, "").getSplits(
                FlightTransactionHandle.INSTANCE, null, TABLE.withConstraint(TupleDomain.none()),
                DynamicFilter.EMPTY, Constraint.alwaysTrue()));
        assertEquals(List.of(), splits);
        verify(service, never()).plan(anyString());
    }

    @Test
    void splitSurvivesJsonRoundTrip() throws Exception {
        // Same auto-detection settings as the airlift mapper Trino uses for splits.
        com.fasterxml.jackson.databind.ObjectMapper mapper = com.fasterxml.jackson.databind.json.JsonMapper.builder()
                .disable(com.fasterxml.jackson.databind.MapperFeature.AUTO_DETECT_CREATORS,
                        com.fasterxml.jackson.databind.MapperFeature.AUTO_DETECT_FIELDS,
                        com.fasterxml.jackson.databind.MapperFeature.AUTO_DETECT_GETTERS,
                        com.fasterxml.jackson.databind.MapperFeature.AUTO_DETECT_IS_GETTERS,
                        com.fasterxml.jackson.databind.MapperFeature.AUTO_DETECT_SETTERS)
                .build();
        FlightSplit split = new FlightSplit(List.of("grpc+tcp://h:1"), new byte[] {0, 1, -1});
        String json = mapper.writeValueAsString(split);
        assertEquals(split, mapper.readValue(json, FlightSplit.class));
        assertTrue(json.contains("\"ticket\""));

        // Trino writes types as their signature; a two-type stand-in is enough here.
        com.fasterxml.jackson.databind.module.SimpleModule types = new com.fasterxml.jackson.databind.module.SimpleModule();
        types.addSerializer(io.trino.spi.type.Type.class,
                new com.fasterxml.jackson.databind.ser.std.ToStringSerializerBase(io.trino.spi.type.Type.class) {
                    @Override
                    public String valueToString(Object value) {
                        return ((io.trino.spi.type.Type) value).getTypeSignature().toString();
                    }
                });
        types.addDeserializer(io.trino.spi.type.Type.class,
                new com.fasterxml.jackson.databind.deser.std.FromStringDeserializer<>(io.trino.spi.type.Type.class) {
                    @Override
                    protected io.trino.spi.type.Type _deserialize(String value,
                            com.fasterxml.jackson.databind.DeserializationContext context) {
                        return "bigint".equals(value) ? BigintType.BIGINT : io.trino.spi.type.VarcharType.VARCHAR;
                    }
                });
        mapper.registerModule(types);
        FlightColumnHandle column = new FlightColumnHandle("id", "ID", BigintType.BIGINT);
        assertEquals(column, mapper.readValue(mapper.writeValueAsString(column), FlightColumnHandle.class));
    }

    @Test
    void splitCopiesTicketAndReportsSize() {
        byte[] ticket = {9, 8};
        FlightSplit split = new FlightSplit(List.of("grpc+tcp://h:1"), ticket);
        ticket[0] = 0;
        assertArrayEquals(new byte[] {9, 8}, split.ticket());
        split.ticket()[1] = 0;
        assertArrayEquals(new byte[] {9, 8}, split.ticket());
        assertTrue(split.getRetainedSizeInBytes() > 2);
        assertEquals("grpc+tcp://h:1", split.getSplitInfo().get("uris"));
        assertEquals(java.util.Map.of("uris", List.of("grpc+tcp://h:1")), split.getInfo());
        assertEquals(split.hashCode(), new FlightSplit(List.of("grpc+tcp://h:1"), new byte[] {9, 8}).hashCode());
        assertNotEquals(split, new FlightSplit(List.of("grpc+tcp://h:1"), new byte[] {9}));
        assertNotEquals(split, "other");
        assertTrue(split.toString().contains("ticketBytes=2"));
    }
}
