package pro.surpin.data.arrowflight.trino;

import io.airlift.slice.Slices;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.predicate.ValueSet;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.DateType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.Int128;
import io.trino.spi.type.RealType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import pro.surpin.data.arrowflight.client.expr.Expr;
import pro.surpin.data.arrowflight.client.expr.SqlRenderer;

import java.math.BigInteger;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;

@Tag("unit")
class FlightFiltersTest {
    private static final SqlRenderer RENDERER = new SqlRenderer("");

    private static FlightColumnHandle column(String name, Type type) {
        return new FlightColumnHandle(name.toLowerCase(java.util.Locale.ROOT), name, type);
    }

    private static Optional<String> sql(FlightColumnHandle column, Domain domain) {
        TupleDomain<ColumnHandle> tuple = TupleDomain.withColumnDomains(Map.of(column, domain));
        return FlightFilters.toExpr(tuple).map(RENDERER::render);
    }

    @Test
    void rendersEqualityAndInList() {
        FlightColumnHandle id = column("Id", BigintType.BIGINT);
        assertEquals(Optional.of("Id = 5"), sql(id, Domain.singleValue(BigintType.BIGINT, 5L)));
        assertEquals(Optional.of("Id in (1,2,3)"),
                sql(id, Domain.multipleValues(BigintType.BIGINT, List.of(1L, 2L, 3L))));
    }

    @Test
    void rendersRanges() {
        FlightColumnHandle id = column("id", BigintType.BIGINT);
        Domain between = Domain.create(ValueSet.ofRanges(
                Range.range(BigintType.BIGINT, 1L, true, 10L, false)), false);
        assertEquals(Optional.of("(id >= 1 and id < 10)"), sql(id, between));

        Domain twoSided = Domain.create(ValueSet.ofRanges(
                Range.lessThan(BigintType.BIGINT, 0L), Range.greaterThanOrEqual(BigintType.BIGINT, 100L)), false);
        assertEquals(Optional.of("(id < 0 or id >= 100)"), sql(id, twoSided));
    }

    @Test
    void rendersNullHandling() {
        FlightColumnHandle id = column("id", BigintType.BIGINT);
        assertEquals(Optional.of("id is null"), sql(id, Domain.onlyNull(BigintType.BIGINT)));
        assertEquals(Optional.of("id is not null"), sql(id, Domain.notNull(BigintType.BIGINT)));
        assertEquals(Optional.of("(id = 1 or id is null)"),
                sql(id, Domain.singleValue(BigintType.BIGINT, 1L, true)));
    }

    @Test
    void rendersNoneAsFalse() {
        assertEquals(Optional.of("(1 = 0)"), FlightFilters.toExpr(TupleDomain.none()).map(RENDERER::render));
        assertEquals(Optional.empty(), FlightFilters.toExpr(TupleDomain.all()));
        assertEquals(Optional.of(new Expr.Constant(false)),
                FlightFilters.columnFilter(new Expr.Column("c"), Domain.none(BigintType.BIGINT)));
        assertEquals(Optional.empty(), FlightFilters.columnFilter(new Expr.Column("c"), Domain.all(BigintType.BIGINT)));
    }

    @Test
    void combinesColumnsWithAnd() {
        FlightColumnHandle a = column("a", BigintType.BIGINT);
        FlightColumnHandle b = column("b", BigintType.BIGINT);
        TupleDomain<ColumnHandle> tuple = TupleDomain.withColumnDomains(Map.of(
                a, Domain.singleValue(BigintType.BIGINT, 1L),
                b, Domain.singleValue(BigintType.BIGINT, 2L)));
        String rendered = FlightFilters.toExpr(tuple).map(RENDERER::render).orElseThrow();
        assertEquals(true, rendered.equals("(a = 1 and b = 2)") || rendered.equals("(b = 2 and a = 1)"));
    }

    @Test
    void rendersVarcharEqualityButNotRanges() {
        FlightColumnHandle name = column("name", VarcharType.VARCHAR);
        assertEquals(Optional.of("name = 'O''Brien'"),
                sql(name, Domain.singleValue(VarcharType.VARCHAR, Slices.utf8Slice("O'Brien"))));
        Domain range = Domain.create(ValueSet.ofRanges(
                Range.greaterThan(VarcharType.VARCHAR, Slices.utf8Slice("m"))), false);
        assertEquals(Optional.empty(), sql(name, range));
    }

    @Test
    void rendersDecimalAndDateLiterals() {
        DecimalType shortDecimal = DecimalType.createDecimalType(10, 2);
        DecimalType longDecimal = DecimalType.createDecimalType(30, 3);
        assertEquals(Optional.of("price = 123.45"),
                sql(column("price", shortDecimal), Domain.singleValue(shortDecimal, 12345L)));
        assertEquals(Optional.of("big = 1.500"),
                sql(column("big", longDecimal), Domain.singleValue(longDecimal, Int128.valueOf(BigInteger.valueOf(1500)))));
        assertEquals(Optional.of("day = '2024-01-02'"),
                sql(column("day", DateType.DATE), Domain.singleValue(DateType.DATE, 19_724L)));
    }

    @Test
    void skipsRealColumns() {
        long bits = Float.floatToRawIntBits(1.1f);
        assertEquals(Optional.empty(), sql(column("r", RealType.REAL), Domain.singleValue(RealType.REAL, bits)));
    }

    @Test
    void tableHandleHelpersKeepOtherFields() {
        FlightTableHandle handle = FlightTableHandle.of(new SchemaTableName("s", "t"), "S", "T");
        FlightColumnHandle id = column("id", BigintType.BIGINT);
        FlightTableHandle projected = handle.withColumns(List.of(id));
        FlightTableHandle filtered = projected.withConstraint(TupleDomain.none());
        assertEquals(Optional.of(List.of(id)), filtered.columns());
        assertEquals("S", filtered.remoteSchema());
        assertEquals(true, filtered.constraint().isNone());
    }
}
