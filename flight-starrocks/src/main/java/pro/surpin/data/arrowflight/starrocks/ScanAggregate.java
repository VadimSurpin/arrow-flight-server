package pro.surpin.data.arrowflight.starrocks;

import java.util.List;
import java.util.Objects;

/**
 * Partial aggregation computed by the Flight server for each endpoint. Callers merge the per-endpoint partials.
 *
 * @param groupKeys plain grouping columns, possibly empty
 * @param partials aggregates to compute, at least one
 */
public record ScanAggregate(List<String> groupKeys, List<Partial> partials) {
    /**
     * Supported partial functions. SUM always yields DOUBLE: the server streams a plain SUM of an integer column as
     * DECIMAL(38,0) even though it plans it as DOUBLE, and rejects a CAST around the aggregate, so the input is cast.
     */
    public enum Func { COUNT_STAR, COUNT, SUM, MIN, MAX }

    /**
     * One aggregate output.
     *
     * @param func function
     * @param column input column, null only for COUNT_STAR
     * @param alias output column name (matches the StarRocks slot name)
     */
    public record Partial(Func func, String column, String alias) {
        public Partial {
            Objects.requireNonNull(func, "func");
            Objects.requireNonNull(alias, "alias");
            if ((func == Func.COUNT_STAR) != (column == null)) {
                throw new IllegalArgumentException(func == Func.COUNT_STAR
                        ? "COUNT(*) takes no column" : func + " needs a column");
            }
        }

        String toSql() {
            String name = ScanSql.identifier(alias);
            return switch (func) {
                case COUNT_STAR -> "COUNT(*) AS " + name;
                case COUNT -> "COUNT(" + ScanSql.identifier(column) + ") AS " + name;
                case SUM -> "SUM(CAST(" + ScanSql.identifier(column) + " AS DOUBLE)) AS " + name;
                case MIN -> "MIN(" + ScanSql.identifier(column) + ") AS " + name;
                case MAX -> "MAX(" + ScanSql.identifier(column) + ") AS " + name;
            };
        }
    }

    public ScanAggregate {
        groupKeys = List.copyOf(Objects.requireNonNull(groupKeys, "groupKeys"));
        partials = List.copyOf(Objects.requireNonNull(partials, "partials"));
        if (partials.isEmpty()) {
            throw new IllegalArgumentException("An aggregate needs at least one partial");
        }
    }
}
