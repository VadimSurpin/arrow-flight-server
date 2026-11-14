package pro.surpin.data.arrowflight.starrocks;

import java.util.List;
import java.util.Objects;

/**
 * Aggregation computed by the Flight server for each endpoint. Callers merge the per-endpoint partials, unless
 * {@code completeBy} names a grouping column whose values the server keeps in one endpoint: then every group is final.
 *
 * @param groupKeys plain grouping columns, possibly empty
 * @param partials aggregates to compute, at least one
 * @param completeBy grouping column the server must keep whole (a plain identifier), or null for partial results
 */
public record ScanAggregate(List<String> groupKeys, List<Partial> partials, String completeBy) {
    /**
     * A partial aggregate: every endpoint answers for its own files.
     *
     * @param groupKeys plain grouping columns, possibly empty
     * @param partials aggregates to compute, at least one
     */
    public ScanAggregate(List<String> groupKeys, List<Partial> partials) {
        this(groupKeys, partials, null);
    }

    /**
     * Supported partial functions. SUM carries whatever cast its input needs: the server streams a plain SUM of an
     * integer column as DECIMAL(38,0) while planning it as DOUBLE, and rejects a CAST around the aggregate, so the
     * cast goes inside (DOUBLE for floating sums, DECIMAL(38,s) for exact decimal sums).
     */
    public enum Func { COUNT_STAR, COUNT, SUM, MIN, MAX }

    /**
     * One aggregate output.
     *
     * @param func function
     * @param input aggregated expression, null only for COUNT_STAR
     * @param alias output column name (matches the StarRocks slot name)
     */
    public record Partial(Func func, ScanExpr input, String alias) {
        public Partial {
            Objects.requireNonNull(func, "func");
            Objects.requireNonNull(alias, "alias");
            if ((func == Func.COUNT_STAR) != (input == null)) {
                throw new IllegalArgumentException(func == Func.COUNT_STAR
                        ? "COUNT(*) takes no input" : func + " needs an input");
            }
        }

        /**
         * A partial over one plain column. SUM casts the column to DOUBLE.
         *
         * @param func function
         * @param column input column, null only for COUNT_STAR
         * @param alias output column name
         * @return partial
         */
        public static Partial column(Func func, String column, String alias) {
            if (column == null) {
                return new Partial(func, null, alias);
            }
            ScanExpr input = new ScanExpr.Column(column);
            return new Partial(func, func == Func.SUM ? ScanExpr.Cast.toDouble(input) : input, alias);
        }

        String toSql() {
            String name = ScanSql.identifier(alias);
            return switch (func) {
                case COUNT_STAR -> "COUNT(*) AS " + name;
                case COUNT -> "COUNT(" + input.toSql() + ") AS " + name;
                case SUM -> "SUM(" + input.toSql() + ") AS " + name;
                case MIN -> "MIN(" + input.toSql() + ") AS " + name;
                case MAX -> "MAX(" + input.toSql() + ") AS " + name;
            };
        }
    }

    public ScanAggregate {
        groupKeys = List.copyOf(Objects.requireNonNull(groupKeys, "groupKeys"));
        partials = List.copyOf(Objects.requireNonNull(partials, "partials"));
        if (partials.isEmpty()) {
            throw new IllegalArgumentException("An aggregate needs at least one partial");
        }
        if (completeBy != null && (!completeBy.matches("[A-Za-z_][A-Za-z0-9_]*") || !groupKeys.contains(completeBy))) {
            throw new IllegalArgumentException("completeBy must be a plain grouping column: " + completeBy);
        }
    }
}
