package pro.surpin.data.arrowflight.trino.pushdown;

import pro.surpin.data.arrowflight.trino.metadata.FlightColumnHandle;

import io.airlift.slice.Slice;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.BooleanType;
import io.trino.spi.type.DateType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.Int128;
import io.trino.spi.type.IntegerType;
import io.trino.spi.type.SmallintType;
import io.trino.spi.type.TinyintType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;
import pro.surpin.data.arrowflight.client.expr.Expr;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Translates a Trino TupleDomain into a Flight server filter.
 *
 * The translation may drop parts it cannot express, so the result is never stricter than the
 * domain; Trino keeps evaluating the full domain on the returned rows.
 */
final class FlightFilters {
    /** Prevents construction of a utility class. */
    private FlightFilters() { }

    /**
     * Translates a TupleDomain.
     *
     * @param domain filter over Flight column handles
     * @return filter expression, or empty when nothing can be pushed
     */
    static Optional<Expr> toExpr(TupleDomain<ColumnHandle> domain) {
        if (domain.isNone()) {
            return Optional.of(new Expr.Constant(false));
        }
        List<Expr> conjuncts = new ArrayList<>();
        for (Map.Entry<ColumnHandle, Domain> entry : domain.getDomains().orElseThrow().entrySet()) {
            FlightColumnHandle column = (FlightColumnHandle) entry.getKey();
            columnFilter(new Expr.Column(column.remoteName()), entry.getValue()).ifPresent(conjuncts::add);
        }
        return conjuncts.stream().reduce(Expr.And::new);
    }

    /**
     * Translates the domain of one column.
     *
     * @param column column reference
     * @param domain column domain
     * @return filter, or empty when the domain cannot be pushed
     */
    static Optional<Expr> columnFilter(Expr.Column column, Domain domain) {
        if (domain.isAll()) {
            return Optional.empty();
        }
        if (domain.isNone()) {
            return Optional.of(new Expr.Constant(false));
        }
        if (domain.getValues().isNone()) {
            return Optional.of(new Expr.IsNull(column, false));
        }
        if (domain.getValues().isAll()) {
            return Optional.of(new Expr.IsNull(column, true));
        }
        Optional<Expr> values = valuesFilter(column, domain);
        if (values.isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(domain.isNullAllowed()
                ? new Expr.Or(values.get(), new Expr.IsNull(column, false))
                : values.get());
    }

    /**
     * Translates the non-null values of a domain.
     *
     * @param column column reference
     * @param domain column domain with some but not all values
     * @return filter, or empty when the values cannot be pushed
     */
    private static Optional<Expr> valuesFilter(Expr.Column column, Domain domain) {
        Type type = domain.getType();
        if (!isPushable(type)) {
            return Optional.empty();
        }
        List<Range> ranges = domain.getValues().getRanges().getOrderedRanges();
        if (ranges.stream().allMatch(Range::isSingleValue)) {
            List<Expr.Literal> literals = new ArrayList<>();
            for (Range range : ranges) {
                Optional<Expr.Literal> literal = literal(type, range.getSingleValue());
                if (literal.isEmpty()) {
                    return Optional.empty();
                }
                literals.add(literal.get());
            }
            return Optional.of(literals.size() == 1
                    ? new Expr.Compare(Expr.CompareOp.EQ, column, literals.getFirst())
                    : new Expr.In(column, literals));
        }
        if (type instanceof VarcharType) {
            return Optional.empty();
        }
        List<Expr> disjuncts = new ArrayList<>();
        for (Range range : ranges) {
            Optional<Expr> rangeFilter = rangeFilter(column, type, range);
            if (rangeFilter.isEmpty()) {
                return Optional.empty();
            }
            disjuncts.add(rangeFilter.get());
        }
        return disjuncts.stream().reduce(Expr.Or::new);
    }

    /**
     * Translates one range.
     *
     * @param column column reference
     * @param type column type
     * @param range range
     * @return filter, or empty when a bound cannot be rendered
     */
    private static Optional<Expr> rangeFilter(Expr.Column column, Type type, Range range) {
        if (range.isSingleValue()) {
            return literal(type, range.getSingleValue())
                    .map(value -> new Expr.Compare(Expr.CompareOp.EQ, column, value));
        }
        List<Expr> bounds = new ArrayList<>();
        if (range.getLowValue().isPresent()) {
            Optional<Expr.Literal> low = literal(type, range.getLowValue().get());
            if (low.isEmpty()) {
                return Optional.empty();
            }
            bounds.add(new Expr.Compare(range.isLowInclusive() ? Expr.CompareOp.GE : Expr.CompareOp.GT,
                    column, low.get()));
        }
        if (range.getHighValue().isPresent()) {
            Optional<Expr.Literal> high = literal(type, range.getHighValue().get());
            if (high.isEmpty()) {
                return Optional.empty();
            }
            bounds.add(new Expr.Compare(range.isHighInclusive() ? Expr.CompareOp.LE : Expr.CompareOp.LT,
                    column, high.get()));
        }
        return bounds.stream().reduce(Expr.And::new);
    }

    /**
     * Checks whether filters on a type are pushed.
     * REAL is excluded: a float column compared with a decimal literal can drop matching rows.
     *
     * @param type column type
     * @return true when pushed
     */
    private static boolean isPushable(Type type) {
        return type instanceof BooleanType || type instanceof TinyintType || type instanceof SmallintType
                || type instanceof IntegerType || type instanceof BigintType || type instanceof DoubleType
                || type instanceof DecimalType || type instanceof DateType || type instanceof VarcharType;
    }

    /**
     * Converts a Trino value to a SQL literal.
     *
     * @param type value type
     * @param value Trino native value
     * @return literal, or empty when the value has no exact SQL form
     */
    static Optional<Expr.Literal> literal(Type type, Object value) {
        Object javaValue = switch (type) {
            case DecimalType decimal -> new BigDecimal(decimal.isShort()
                    ? BigInteger.valueOf((Long) value) : ((Int128) value).toBigInteger(), decimal.getScale());
            case DateType ignored -> LocalDate.ofEpochDay((Long) value);
            case VarcharType ignored -> ((Slice) value).toStringUtf8();
            default -> value;
        };
        return Expr.Literal.of(javaValue);
    }
}
