package pro.surpin.data.arrowflight.starrocks;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoField;
import java.util.List;
import java.util.Objects;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/** A filter the frontend may push into the Flight SQL query; renders to restricted SQL. */
public sealed interface ScanPredicate {
    /** Identifier rule shared with {@link ScanSql}. */
    Pattern IDENTIFIER = Pattern.compile("[A-Za-z_][A-Za-z0-9_]*");

    /** Comparison operators. */
    enum Op {
        EQ("="), NE("<>"), LT("<"), LE("<="), GT(">"), GE(">=");

        private final String sql;

        Op(String sql) {
            this.sql = sql;
        }

        String sql() {
            return sql;
        }
    }

    /** A constant. */
    sealed interface Literal {
        /**
         * Renders this constant as SQL.
         *
         * @return SQL literal
         */
        String toSql();
    }

    /**
     * Integer constant.
     *
     * @param value the value
     */
    record IntLit(long value) implements Literal {
        @Override
        public String toSql() {
            return Long.toString(value);
        }
    }

    /**
     * Exact decimal constant.
     *
     * @param value the value
     */
    record DecimalLit(BigDecimal value) implements Literal {
        @Override
        public String toSql() {
            return value.toPlainString();
        }
    }

    /**
     * Floating point constant; NaN and infinities cannot be pushed.
     *
     * @param value the value
     */
    record DoubleLit(double value) implements Literal {
        @Override
        public String toSql() {
            if (Double.isNaN(value) || Double.isInfinite(value)) {
                throw new IllegalArgumentException("Cannot push non-finite double");
            }
            return Double.toString(value);
        }
    }

    /**
     * String constant. The server's SQL engine (DuckDB) treats a backslash as an
     * ordinary character, so only the single quote needs escaping.
     *
     * @param value the value
     */
    record StringLit(String value) implements Literal {
        @Override
        public String toSql() {
            return "'" + value.replace("'", "''") + "'";
        }
    }

    /**
     * Boolean constant.
     *
     * @param value the value
     */
    record BoolLit(boolean value) implements Literal {
        @Override
        public String toSql() {
            return value ? "TRUE" : "FALSE";
        }
    }

    /**
     * Date constant.
     *
     * @param value the value
     */
    record DateLit(LocalDate value) implements Literal {
        @Override
        public String toSql() {
            return "DATE '" + value + "'";
        }
    }

    /**
     * Timestamp constant without time zone.
     *
     * @param value the value
     */
    record TimestampLit(LocalDateTime value) implements Literal {
        private static final DateTimeFormatter FORMAT = new DateTimeFormatterBuilder()
                .appendPattern("yyyy-MM-dd HH:mm:ss")
                .optionalStart().appendFraction(ChronoField.NANO_OF_SECOND, 1, 9, true)
                .optionalEnd().toFormatter();

        @Override
        public String toSql() {
            return "TIMESTAMP '" + FORMAT.format(value) + "'";
        }
    }

    /**
     * {@code column op literal}.
     *
     * @param column column name
     * @param op comparison operator
     * @param value constant
     */
    record Compare(String column, Op op, Literal value) implements ScanPredicate {
        @Override
        public String toSql() {
            return "(" + ident(column) + " " + op.sql() + " " + value.toSql() + ")";
        }
    }

    /**
     * {@code column IN (...)}.
     *
     * @param column column name
     * @param values constants
     */
    record In(String column, List<Literal> values) implements ScanPredicate {
        @Override
        public String toSql() {
            if (values.isEmpty()) {
                throw new IllegalArgumentException("IN list must not be empty");
            }
            return "(" + ident(column) + " IN ("
                    + values.stream().map(Literal::toSql).collect(Collectors.joining(", ")) + "))";
        }
    }

    /**
     * {@code column IS [NOT] NULL}.
     *
     * @param column column name
     * @param negated true for IS NOT NULL
     */
    record IsNull(String column, boolean negated) implements ScanPredicate {
        @Override
        public String toSql() {
            return "(" + ident(column) + (negated ? " IS NOT NULL)" : " IS NULL)");
        }
    }

    /**
     * Conjunction.
     *
     * @param terms operands
     */
    record And(List<ScanPredicate> terms) implements ScanPredicate {
        @Override
        public String toSql() {
            return join(terms, " AND ");
        }
    }

    /**
     * Disjunction.
     *
     * @param terms operands
     */
    record Or(List<ScanPredicate> terms) implements ScanPredicate {
        @Override
        public String toSql() {
            return join(terms, " OR ");
        }
    }

    /**
     * Negation.
     *
     * @param term operand
     */
    record Not(ScanPredicate term) implements ScanPredicate {
        @Override
        public String toSql() {
            return "(NOT " + term.toSql() + ")";
        }
    }

    /**
     * Columns a predicate reads, in first-use order without duplicates.
     *
     * @param predicate predicate to inspect
     * @return column names
     */
    static List<String> columnsOf(ScanPredicate predicate) {
        List<String> found = new java.util.ArrayList<>();
        collectColumns(predicate, found);
        return found;
    }

    private static void collectColumns(ScanPredicate predicate, List<String> found) {
        String name = null;
        if (predicate instanceof Compare c) {
            name = c.column();
        } else if (predicate instanceof In i) {
            name = i.column();
        } else if (predicate instanceof IsNull n) {
            name = n.column();
        } else if (predicate instanceof And a) {
            a.terms().forEach(t -> collectColumns(t, found));
        } else if (predicate instanceof Or o) {
            o.terms().forEach(t -> collectColumns(t, found));
        } else if (predicate instanceof Not n) {
            collectColumns(n.term(), found);
        }
        if (name != null && !found.contains(name)) {
            found.add(name);
        }
    }

    /**
     * Renders this predicate as SQL.
     *
     * @return SQL boolean expression
     */
    String toSql();

    private static String ident(String name) {
        Objects.requireNonNull(name, "column");
        if (!IDENTIFIER.matcher(name).matches()) {
            throw new IllegalArgumentException("Unsupported SQL identifier: " + name);
        }
        return name;
    }

    private static String join(List<ScanPredicate> terms, String separator) {
        if (terms.isEmpty()) {
            throw new IllegalArgumentException("Boolean predicate needs at least one term");
        }
        return "(" + terms.stream().map(ScanPredicate::toSql)
                .collect(Collectors.joining(separator)) + ")";
    }
}
