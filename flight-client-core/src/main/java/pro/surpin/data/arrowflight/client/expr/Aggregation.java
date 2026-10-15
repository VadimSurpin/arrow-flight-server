package pro.surpin.data.arrowflight.client.expr;

import java.io.Serializable;
import java.util.List;
import java.util.Objects;

/**
 * Engine-neutral aggregation pushed to the Flight server: grouping columns plus aggregate calls.
 * @param groupBy grouping columns, possibly empty
 * @param aggregates aggregate calls, possibly empty when only grouping is pushed
 */
public record Aggregation(List<Expr.Column> groupBy, List<Call> aggregates) implements Serializable {

    /** Supported aggregate functions. */
    public enum Function {
        /** COUNT(*). */
        COUNT_STAR,
        /** COUNT(expr). */
        COUNT,
        /** MIN(expr). */
        MIN,
        /** MAX(expr). */
        MAX,
        /** SUM(expr). */
        SUM
    }

    /**
     * One aggregate call.
     * @param function aggregate function
     * @param argument function argument, null only for COUNT_STAR
     */
    public record Call(Function function, Expr argument) implements Serializable {
        /**
         * Validates that only COUNT_STAR has no argument.
         * @param function aggregate function
         * @param argument function argument
         */
        public Call {
            Objects.requireNonNull(function, "function");
            if ((function == Function.COUNT_STAR) != (argument == null)) {
                throw new IllegalArgumentException("COUNT_STAR takes no argument; other functions need one");
            }
        }
    }

    /**
     * Copies the lists.
     * @param groupBy grouping columns
     * @param aggregates aggregate calls
     */
    public Aggregation {
        groupBy = List.copyOf(groupBy);
        aggregates = List.copyOf(aggregates);
    }

    /**
     * Returns true when nothing would be pushed.
     * @return true when there are neither grouping columns nor aggregates
     */
    public boolean isEmpty() {
        return groupBy.isEmpty() && aggregates.isEmpty();
    }
}
