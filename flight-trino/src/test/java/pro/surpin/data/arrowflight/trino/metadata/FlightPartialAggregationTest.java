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
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.DecimalType.createDecimalType;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertThrows;
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
        assertEquals(List.of(KEY, table.aggregation().orElseThrow().aggregates().getFirst().output(),
                table.aggregation().orElseThrow().aggregates().getLast().output()),
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

    /** Checks all supported functions expose native states and physical Arrow expressions. */
    @Test
    void describesNumericAggregates() {
        FlightColumnHandle floating = new FlightColumnHandle("d", "D", DOUBLE);
        Type averageType = io.trino.spi.type.RowType.anonymous(List.of(DOUBLE, BIGINT));
        List<AggregateFunction> functions = List.of(
                aggregate("min", BIGINT, "k", BIGINT), aggregate("max", BIGINT, "k", BIGINT),
                aggregate("sum", BIGINT, "k", BIGINT), aggregate("sum", DOUBLE, "d", DOUBLE),
                aggregate("avg", DOUBLE, "k", BIGINT), aggregate("avg", DOUBLE, "d", DOUBLE));
        var result = new FlightMetadata(mock(FlightService.class)).applyPartialAggregation(session(true), TABLE,
                functions, Map.of("k", KEY, "d", floating), List.of(List.of(KEY)),
                List.of(BIGINT, BIGINT, BIGINT, DOUBLE, averageType, averageType)).orElseThrow();
        FlightTableHandle handle = (FlightTableHandle) result.getHandle();
        assertEquals("SELECT Key, MIN(Key) AS __flight_min_0, MAX(Key) AS __flight_max_1, "
                + "SUM(CAST(Key AS DECIMAL(19, 0))) AS __flight_sum_2, SUM(D) AS __flight_sum_3, "
                + "SUM(CAST(Key AS DOUBLE)) AS __flight_avg_4_sum, COUNT(Key) AS __flight_avg_4_count, "
                + "SUM(D) AS __flight_avg_5_sum, COUNT(D) AS __flight_avg_5_count FROM S.T GROUP BY Key",
                FlightQueries.select(handle, ""));
        var specifications = handle.aggregation().orElseThrow().aggregates();
        assertEquals(AggregateSpec.StateFormat.LONG_SUM, specifications.get(2).stateFormat());
        assertEquals(AggregateSpec.EmptyState.NULL, specifications.get(0).emptyState());
        assertEquals(AggregateSpec.EmptyState.AVERAGE_ZERO, specifications.get(4).emptyState());
        assertEquals(averageType, result.getProjections().get(4).getType());
    }

    /** Checks all supported integer widths retain their extrema and use native BIGINT SUM states. */
    @Test
    void acceptsSupportedIntegerWidths() {
        for (Type type : List.of(BIGINT, io.trino.spi.type.IntegerType.INTEGER,
                io.trino.spi.type.SmallintType.SMALLINT, io.trino.spi.type.TinyintType.TINYINT)) {
            FlightColumnHandle input = new FlightColumnHandle("v", "V", type);
            Type averageType = io.trino.spi.type.RowType.anonymous(List.of(DOUBLE, BIGINT));
            var result = new FlightMetadata(mock(FlightService.class)).applyPartialAggregation(session(true), TABLE,
                    List.of(aggregate("min", type, "v", type), aggregate("max", type, "v", type),
                            aggregate("sum", BIGINT, "v", type), aggregate("avg", DOUBLE, "v", type)),
                    Map.of("v", input), List.of(List.of()), List.of(type, type, BIGINT, averageType));
            assertTrue(result.isPresent(), type.toString());
        }
    }

    /** Checks AVG aliases cannot collide with a grouping column or another physical state. */
    @Test
    void avoidsAveragePhysicalAliasCollision() {
        FlightColumnHandle group = new FlightColumnHandle("group", "__flight_avg_0_sum", BIGINT);
        Type averageType = io.trino.spi.type.RowType.anonymous(List.of(DOUBLE, BIGINT));
        var result = new FlightMetadata(mock(FlightService.class)).applyPartialAggregation(session(true), TABLE,
                List.of(aggregate("avg", DOUBLE, "k", BIGINT)), Map.of("k", KEY),
                List.of(List.of(group)), List.of(averageType)).orElseThrow();
        assertEquals(new Variable("__flight_avg_0_", averageType), result.getProjections().getFirst());
    }

    /** Checks numeric extrema preserve their native scalar types and render NaN-safe maxima. */
    @Test
    void acceptsFloatingAndDecimalExtrema() {
        for (Type type : List.of(REAL, DOUBLE, createDecimalType(9, 2), createDecimalType(18, 2),
                createDecimalType(19, 3), createDecimalType(38, 18), createDecimalType(38, 0))) {
            FlightColumnHandle column = new FlightColumnHandle("v", "V", type);
            for (String function : List.of("min", "max")) {
                var result = new FlightMetadata(mock(FlightService.class)).applyPartialAggregation(session(true), TABLE,
                        List.of(aggregate(function, type, "v", type)), Map.of("v", column),
                        List.of(List.of()), List.of(type)).orElseThrow();
                var handle = (FlightTableHandle) result.getHandle();
                assertEquals(type, result.getProjections().getFirst().getType());
                boolean negate = function.equals("max") && (type.equals(REAL) || type.equals(DOUBLE));
                assertEquals("SELECT " + (negate ? "MIN(-(V))" : function.toUpperCase(java.util.Locale.ROOT) + "(V)")
                                + " AS __flight_" + function + "_0 FROM S.T",
                        FlightQueries.select(handle, ""));
                assertEquals(negate ? AggregateSpec.StateFormat.NEGATED_EXTREMUM : AggregateSpec.StateFormat.SCALAR,
                        handle.aggregation().orElseThrow().aggregates().getFirst().stateFormat());
            }
        }
    }

    /** Checks incompatible serializers and nonnumeric extrema retain the original plan. */
    @Test
    void rejectsIncompatibleNumericStates() {
        FlightMetadata metadata = new FlightMetadata(mock(FlightService.class));
        for (Type type : List.of(VARCHAR, io.trino.spi.type.DateType.DATE, io.trino.spi.type.BooleanType.BOOLEAN,
                io.trino.spi.type.VarbinaryType.VARBINARY, new io.trino.spi.type.ArrayType(BIGINT))) {
            FlightColumnHandle column = new FlightColumnHandle("v", "V", type);
            for (String function : List.of("min", "max")) {
                assertTrue(metadata.applyPartialAggregation(session(true), TABLE,
                        List.of(aggregate(function, type, "v", type)), Map.of("v", column),
                        List.of(List.of()), List.of(type)).isEmpty());
            }
        }
        assertTrue(metadata.applyPartialAggregation(session(true), TABLE,
                List.of(aggregate("avg", DOUBLE, "k", BIGINT)), INPUTS, List.of(List.of()),
                List.of(io.trino.spi.type.RowType.anonymous(List.of(BIGINT, DOUBLE)))).isEmpty());
        assertTrue(metadata.applyPartialAggregation(session(true), TABLE,
                List.of(aggregate("min", DOUBLE, "k", BIGINT)), INPUTS, List.of(List.of()), List.of(BIGINT)).isEmpty());
        assertThrows(IllegalArgumentException.class, () -> new AggregateSpec(AggregateSpec.Function.AVG,
                Optional.of(KEY), new FlightColumnHandle("avg", "avg", DOUBLE)));
    }

    /**
     * Creates a unary aggregate request.
     *
     * @param name function name
     * @param output result type
     * @param input input variable
     * @param inputType input type
     * @return aggregate request
     */
    private static AggregateFunction aggregate(String name, Type output, String input, Type inputType) {
        return new AggregateFunction(name, output, List.of(new Variable(input, inputType)), List.of(), false, Optional.empty());
    }

    /** Checks unsupported functions, expressions, types and grouping sets. */
    @Test
    void rejectsUnsupportedContracts() {
        FlightMetadata metadata = new FlightMetadata(mock(FlightService.class));
        List<AggregateFunction> unsupported = List.of(
                new AggregateFunction("median", BIGINT, List.of(new Variable("k", BIGINT)), List.of(), false, Optional.empty()),
                new AggregateFunction("min", BIGINT, List.of(), List.of(), false, Optional.empty()),
                new AggregateFunction("sum", BIGINT, List.of(new Variable("v", VARCHAR)), List.of(), false, Optional.empty()),
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
