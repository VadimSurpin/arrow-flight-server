package pro.surpin.data.arrowflight.trino.metadata;

import pro.surpin.data.arrowflight.trino.client.FlightService;
import pro.surpin.data.arrowflight.trino.pushdown.FlightQueries;

import io.trino.spi.connector.AggregateFunction;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.expression.ConnectorExpression;
import io.trino.spi.expression.Variable;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.Type;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/** Checks partial count contracts and aggregate scan SQL. */
@Tag("unit")
class FlightPartialAggregationTest {
    private static final FlightColumnHandle KEY = new FlightColumnHandle("key", "Key", BIGINT);
    private static final FlightColumnHandle VALUE = new FlightColumnHandle("value", "Value", VARCHAR);
    private static final FlightTableHandle TABLE = FlightTableHandle.of(new SchemaTableName("s", "t"), "S", "T");
    private static final Map<String, ColumnHandle> INPUTS = Map.of("k", KEY, "v", VALUE);

    /**
     * Creates a session with the partial pushdown setting.
     *
     * @param enabled partial pushdown setting
     * @return session
     */
    private static ConnectorSession session(boolean enabled) {
        ConnectorSession session = mock(ConnectorSession.class);
        when(session.getProperty("partial_aggregation_pushdown", Boolean.class)).thenReturn(enabled);
        return session;
    }

    /**
     * Creates a count aggregate.
     *
     * @param arguments count arguments
     * @return aggregate
     */
    private static AggregateFunction count(List<ConnectorExpression> arguments) {
        return new AggregateFunction("count", BIGINT, arguments, List.of(), false, Optional.empty());
    }

    /** Checks multiple count states, filters, grouping and quoted SQL. */
    @Test
    void buildsCountStatesAndGroupedSql() {
        FlightMetadata metadata = new FlightMetadata(mock(FlightService.class));
        var filter = TupleDomain.<ColumnHandle>withColumnDomains(Map.of(KEY, Domain.singleValue(BIGINT, 7L)));
        var result = metadata.applyPartialAggregation(session(true), TABLE.withConstraint(filter),
                List.of(count(List.of()), count(List.of(new Variable("v", VARCHAR)))), INPUTS,
                List.of(List.of(KEY)), List.of(BIGINT, BIGINT)).orElseThrow();
        FlightTableHandle table = (FlightTableHandle) result.getHandle();
        assertEquals(3, table.columns().orElseThrow().size());
        assertEquals(List.of(new Variable("__flight_count_0", BIGINT), new Variable("__flight_count_1", BIGINT)),
                result.getProjections());
        assertEquals("SELECT \"Key\", COUNT(*) AS \"__flight_count_0\", COUNT(\"Value\") AS \"__flight_count_1\""
                        + " FROM \"S\".\"T\" WHERE \"Key\" = 7 GROUP BY \"Key\"",
                FlightQueries.select(table, "\""));
        assertEquals("SELECT Key, COUNT(*) AS __flight_count_0, COUNT(Value) AS __flight_count_1"
                        + " FROM S.T WHERE (Key = 7) AND (Key < 10) GROUP BY Key",
                FlightQueries.select(table, "", "Key < 10"));
        assertEquals(List.of(KEY, table.aggregation().orElseThrow().counts().getFirst().output(),
                table.aggregation().orElseThrow().counts().getLast().output()),
                metadata.getColumnHandles(session(true), table).values().stream().toList());
        assertTrue(metadata.getTableStatistics(session(true), table).isEmpty());
        assertTrue(metadata.applyFilter(session(true), table, new Constraint(filter)).isEmpty());
        assertTrue(metadata.applyProjection(session(true), table, List.of(), Map.of()).isEmpty());
        assertTrue(metadata.applyPartialAggregation(session(true), table, List.of(count(List.of())),
                INPUTS, List.of(List.of(KEY)), List.of(BIGINT)).isEmpty());
    }

    /** Checks ungrouped counts and the default disabled setting. */
    @Test
    void buildsGlobalCountOnlyWhenEnabled() {
        FlightMetadata metadata = new FlightMetadata(mock(FlightService.class));
        assertTrue(metadata.applyPartialAggregation(session(false), TABLE, List.of(count(List.of())),
                INPUTS, List.of(List.of()), List.of(BIGINT)).isEmpty());
        var result = metadata.applyPartialAggregation(session(true), TABLE, List.of(count(List.of())),
                INPUTS, List.of(List.of()), List.of(BIGINT)).orElseThrow();
        assertEquals("SELECT COUNT(*) AS __flight_count_0 FROM S.T",
                FlightQueries.select((FlightTableHandle) result.getHandle(), ""));
        assertEquals(result.getAssignments().getFirst().getColumn(),
                ((FlightTableHandle) result.getHandle()).columns().orElseThrow().getFirst());
    }

    /** Checks alias collisions with existing grouping columns. */
    @Test
    void avoidsCountAliasCollision() {
        FlightColumnHandle group = new FlightColumnHandle("__flight_count_0", "__flight_count_0", BIGINT);
        var result = new FlightMetadata(mock(FlightService.class)).applyPartialAggregation(session(true), TABLE,
                List.of(count(List.of())), Map.of("g", group), List.of(List.of(group)), List.of(BIGINT)).orElseThrow();
        assertEquals(new Variable("__flight_count_0_", BIGINT), result.getProjections().getFirst());
    }

    /** Checks unsupported functions, expressions, types and grouping sets. */
    @Test
    void rejectsUnsupportedContracts() {
        FlightMetadata metadata = new FlightMetadata(mock(FlightService.class));
        List<AggregateFunction> unsupported = List.of(
                new AggregateFunction("sum", BIGINT, List.of(new Variable("k", BIGINT)), List.of(), false, Optional.empty()),
                new AggregateFunction("count", BIGINT, List.of(new Variable("k", BIGINT)), List.of(), true, Optional.empty()),
                new AggregateFunction("count", BIGINT, List.of(), List.of(), false, Optional.of(new Variable("k", BIGINT))),
                new AggregateFunction("count", DOUBLE, List.of(), List.of(), false, Optional.empty()),
                count(List.of(new Variable("absent", BIGINT))),
                count(List.of(new Variable("k", VARCHAR))),
                count(List.of(new Variable("k", BIGINT), new Variable("v", VARCHAR))));
        for (AggregateFunction function : unsupported) {
            assertTrue(metadata.applyPartialAggregation(session(true), TABLE, List.of(function), INPUTS,
                    List.of(List.of(KEY)), List.of(BIGINT)).isEmpty(), function.toString());
        }
        assertTrue(metadata.applyPartialAggregation(session(true), TABLE, List.of(count(List.of())), INPUTS,
                List.of(List.of(KEY)), List.<Type>of(DOUBLE)).isEmpty());
        assertTrue(metadata.applyPartialAggregation(session(true), TABLE, List.of(count(List.of())), INPUTS,
                List.of(List.of(KEY)), List.of()).isEmpty());
        assertTrue(metadata.applyPartialAggregation(session(true), TABLE, List.of(count(List.of())), INPUTS,
                List.of(List.of(KEY), List.of()), List.of(BIGINT)).isEmpty());
        assertTrue(metadata.applyPartialAggregation(session(true), TABLE, List.of(), INPUTS,
                List.of(List.of(KEY)), List.of()).isEmpty());
        FlightColumnHandle floating = new FlightColumnHandle("f", "f", DOUBLE);
        assertTrue(metadata.applyPartialAggregation(session(true), TABLE, List.of(count(List.of())), INPUTS,
                List.of(List.of(floating)), List.of(BIGINT)).isEmpty());
    }
}
