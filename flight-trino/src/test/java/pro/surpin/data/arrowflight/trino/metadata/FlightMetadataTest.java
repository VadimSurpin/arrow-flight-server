package pro.surpin.data.arrowflight.trino.metadata;

import pro.surpin.data.arrowflight.trino.FlightConnector;
import pro.surpin.data.arrowflight.trino.client.FlightService;
import pro.surpin.data.arrowflight.trino.read.FlightPageSourceProvider;
import pro.surpin.data.arrowflight.trino.read.FlightSplitManager;

import io.trino.spi.connector.Assignment;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.ConstraintApplicationResult;
import io.trino.spi.connector.ProjectionApplicationResult;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.SchemaTablePrefix;
import io.trino.spi.expression.ConnectorExpression;
import io.trino.spi.expression.Variable;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.VarcharType;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import pro.surpin.data.arrowflight.client.query.QueryEndpoints;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@Tag("unit")
class FlightMetadataTest {
    private static final ConnectorSession SESSION = mock(ConnectorSession.class);
    private static final Schema ORDERS = new Schema(List.of(
            new Field("ID", FieldType.nullable(new ArrowType.Int(64, true)), null),
            new Field("Name", FieldType.nullable(ArrowType.Utf8.INSTANCE), null),
            new Field("half", FieldType.nullable(new ArrowType.FloatingPoint(FloatingPointPrecision.HALF)), null),
            new Field("id", FieldType.nullable(new ArrowType.Int(32, true)), null)));

    private FlightService service;
    private FlightMetadata metadata;

    @BeforeEach
    void setUp() {
        service = mock(FlightService.class);
        when(service.listSchemas()).thenReturn(List.of("Sales", "hr"));
        Map<String, Schema> sales = new LinkedHashMap<>();
        sales.put("Orders", ORDERS);
        when(service.listTables("Sales")).thenReturn(sales);
        when(service.listTables("hr")).thenReturn(Map.of("people", new Schema(List.of())));
        metadata = new FlightMetadata(service);
    }

    private FlightTableHandle orders() {
        return (FlightTableHandle) metadata.getTableHandle(SESSION, new SchemaTableName("sales", "orders"));
    }

    @Test
    void listsLowerCaseSchemasAndTables() {
        assertEquals(List.of("sales", "hr"), metadata.listSchemaNames(SESSION));
        assertEquals(List.of(new SchemaTableName("sales", "orders"), new SchemaTableName("hr", "people")),
                metadata.listTables(SESSION, Optional.empty()));
        assertEquals(List.of(new SchemaTableName("sales", "orders")),
                metadata.listTables(SESSION, Optional.of("sales")));
        assertEquals(List.of(), metadata.listTables(SESSION, Optional.of("missing")));
    }

    @Test
    void resolvesRemoteNamesIgnoringCase() {
        FlightTableHandle handle = orders();
        assertEquals("Sales", handle.remoteSchema());
        assertEquals("Orders", handle.remoteTable());
        assertTrue(handle.constraint().isAll());
        assertNull(metadata.getTableHandle(SESSION, new SchemaTableName("sales", "missing")));
        assertNull(metadata.getTableHandle(SESSION, new SchemaTableName("missing", "orders")));
    }

    @Test
    void cachesTableListingPerTransaction() {
        orders();
        orders();
        verify(service, times(1)).listTables("Sales");
    }

    @Test
    void skipsUnsupportedAndDuplicateColumns() {
        FlightTableHandle handle = orders();
        List<ColumnMetadata> columns = metadata.getTableMetadata(SESSION, handle).getColumns();
        assertEquals(List.of(new ColumnMetadata("id", BigintType.BIGINT), new ColumnMetadata("name", VarcharType.VARCHAR)),
                columns);
        Map<String, ColumnHandle> handles = metadata.getColumnHandles(SESSION, handle);
        assertEquals(new FlightColumnHandle("id", "ID", BigintType.BIGINT), handles.get("id"));
        assertEquals(new ColumnMetadata("name", VarcharType.VARCHAR),
                metadata.getColumnMetadata(SESSION, handle, handles.get("name")));
    }

    @Test
    void listsTableColumnsByPrefix() {
        Map<SchemaTableName, List<ColumnMetadata>> all = metadata.listTableColumns(SESSION, new SchemaTablePrefix());
        assertEquals(2, all.size());
        Map<SchemaTableName, List<ColumnMetadata>> one = metadata.listTableColumns(SESSION,
                new SchemaTablePrefix("sales", "orders"));
        assertEquals(List.of(new SchemaTableName("sales", "orders")), List.copyOf(one.keySet()));
        assertEquals(Map.of(), metadata.listTableColumns(SESSION, new SchemaTablePrefix("sales", "nothing")));
    }

    @Test
    void failsWhenTableDisappears() {
        FlightTableHandle handle = FlightTableHandle.of(new SchemaTableName("sales", "gone"), "Sales", "Gone");
        assertThrows(IllegalStateException.class, () -> metadata.getTableMetadata(SESSION, handle));
    }

    @Test
    void appliesProjection() {
        FlightTableHandle handle = orders();
        FlightColumnHandle name = (FlightColumnHandle) metadata.getColumnHandles(SESSION, handle).get("name");
        List<ConnectorExpression> projections = List.of(new Variable("n", VarcharType.VARCHAR));
        Optional<ProjectionApplicationResult<ConnectorTableHandle>> result =
                metadata.applyProjection(SESSION, handle, projections, Map.of("n", name));

        FlightTableHandle projected = (FlightTableHandle) result.orElseThrow().getHandle();
        assertEquals(Optional.of(List.of(name)), projected.columns());
        Assignment assignment = result.get().getAssignments().getFirst();
        assertEquals("n", assignment.getVariable());
        assertEquals(VarcharType.VARCHAR, assignment.getType());
        assertEquals(projections, result.get().getProjections());

        assertTrue(metadata.applyProjection(SESSION, projected, projections, Map.of("n", name)).isEmpty());
    }

    @Test
    void readsOneColumnWhenNoColumnIsProjected() {
        FlightTableHandle projected = (FlightTableHandle) metadata.applyProjection(SESSION, orders(), List.of(), Map.of())
                .orElseThrow().getHandle();
        assertEquals(Optional.of(List.of(new FlightColumnHandle("id", "ID", BigintType.BIGINT))),
                projected.columns());

        FlightTableHandle empty = (FlightTableHandle) metadata.getTableHandle(SESSION, new SchemaTableName("hr", "people"));
        FlightTableHandle emptyProjected = (FlightTableHandle) metadata.applyProjection(SESSION, empty, List.of(), Map.of())
                .orElseThrow().getHandle();
        assertEquals(Optional.of(List.of()), emptyProjected.columns());
    }

    @Test
    void appliesFilterAndKeepsItForTrino() {
        FlightTableHandle handle = orders();
        ColumnHandle id = metadata.getColumnHandles(SESSION, handle).get("id");
        TupleDomain<ColumnHandle> summary = TupleDomain.withColumnDomains(
                Map.of(id, Domain.singleValue(BigintType.BIGINT, 3L)));

        ConstraintApplicationResult<ConnectorTableHandle> result =
                metadata.applyFilter(SESSION, handle, new Constraint(summary)).orElseThrow();

        assertEquals(summary, ((FlightTableHandle) result.getHandle()).constraint());
        assertEquals(summary, result.getRemainingFilter());
        assertTrue(metadata.applyFilter(SESSION, result.getHandle(), new Constraint(summary)).isEmpty());
    }

    @Test
    void connectorWiresComponents() {
        FlightConnector connector = new FlightConnector(service, "");
        assertEquals(FlightTransactionHandle.INSTANCE, connector.beginTransaction(null, true, true));
        assertTrue(connector.getMetadata(SESSION, FlightTransactionHandle.INSTANCE) instanceof FlightMetadata);
        assertTrue(connector.getSplitManager() instanceof FlightSplitManager);
        assertTrue(connector.getPageSourceProvider() instanceof FlightPageSourceProvider);
        when(service.plan(org.mockito.ArgumentMatchers.anyString()))
                .thenReturn(new QueryEndpoints(ORDERS, new pro.surpin.data.arrowflight.client.query.Endpoint[0]));
        connector.shutdown();
        verify(service).close();
    }
}
