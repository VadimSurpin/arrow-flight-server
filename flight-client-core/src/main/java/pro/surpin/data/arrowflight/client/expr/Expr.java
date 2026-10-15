package pro.surpin.data.arrowflight.client.expr;

import java.io.Serializable;
import java.time.temporal.TemporalAccessor;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * Engine-neutral expression tree that engine adapters build from their own pushdown
 * expressions and that SqlRenderer turns into Flight server SQL.
 */
public sealed interface Expr extends Serializable {

    /**
     * Reference to a single top-level column.
     * @param name column name, unquoted
     */
    record Column(String name) implements Expr {
        /**
         * Validates the column name.
         * @param name column name
         */
        public Column {
            Objects.requireNonNull(name, "name");
        }
    }

    /**
     * Literal value; create it through Literal.of so that unsupported values are rejected.
     * @param value Java value, or null for SQL NULL
     */
    record Literal(Object value) implements Expr {

        /**
         * Creates a literal when the value can be rendered as exact SQL.
         * @param value null, finite Number, Boolean, CharSequence, Character or date/time value
         * @return literal, or empty when the value is not supported
         */
        public static Optional<Literal> of(Object value) {
            return isSupported(value) ? Optional.of(new Literal(value)) : Optional.empty();
        }

        /**
         * Checks whether a value has an exact SQL literal form.
         * @param value candidate value
         * @return true when supported
         */
        private static boolean isSupported(Object value) {
            return switch (value) {
                case null -> true;
                case Double d -> Double.isFinite(d);
                case Float f -> Float.isFinite(f);
                case Number ignored -> true;
                case Boolean ignored -> true;
                case CharSequence ignored -> true;
                case Character ignored -> true;
                case java.sql.Date ignored -> true;
                case java.sql.Time ignored -> true;
                case java.sql.Timestamp ignored -> true;
                case TemporalAccessor ignored -> true;
                default -> false;
            };
        }
    }

    /** Comparison operators. */
    enum CompareOp {
        /** Equal. */
        EQ("="),
        /** Not equal. */
        NE("<>"),
        /** Less than. */
        LT("<"),
        /** Less than or equal. */
        LE("<="),
        /** Greater than. */
        GT(">"),
        /** Greater than or equal. */
        GE(">="),
        /** Null-safe equal: true when both sides are null or both are equal. */
        NULL_SAFE_EQ("<=>");

        private final String symbol;

        /**
         * Creates an operator.
         * @param symbol SQL symbol
         */
        CompareOp(String symbol) {
            this.symbol = symbol;
        }

        /**
         * Returns the SQL operator symbol.
         * @return operator symbol
         */
        public String symbol() {
            return symbol;
        }

        /**
         * Finds an operator by its SQL symbol.
         * @param symbol operator symbol such as "=" or "<="
         * @return operator, or empty when unknown
         */
        public static Optional<CompareOp> fromSymbol(String symbol) {
            for (CompareOp op : values()) {
                if (op.symbol.equals(symbol)) {
                    return Optional.of(op);
                }
            }
            return Optional.empty();
        }
    }

    /**
     * Binary comparison.
     * @param op operator
     * @param left left operand
     * @param right right operand
     */
    record Compare(CompareOp op, Expr left, Expr right) implements Expr {
    }

    /**
     * Logical conjunction.
     * @param left left operand
     * @param right right operand
     */
    record And(Expr left, Expr right) implements Expr {
    }

    /**
     * Logical disjunction.
     * @param left left operand
     * @param right right operand
     */
    record Or(Expr left, Expr right) implements Expr {
    }

    /**
     * Logical negation.
     * @param child negated expression
     */
    record Not(Expr child) implements Expr {
    }

    /**
     * Null check.
     * @param operand checked expression
     * @param negated true for IS NOT NULL
     */
    record IsNull(Expr operand, boolean negated) implements Expr {
    }

    /**
     * Membership test against a literal list.
     * @param operand tested expression
     * @param values literal values, never empty
     */
    record In(Expr operand, List<Literal> values) implements Expr {
        /**
         * Copies the value list.
         * @param operand tested expression
         * @param values literal values
         */
        public In {
            values = List.copyOf(values);
        }
    }

    /** Kinds of LIKE patterns built from a plain string. */
    enum LikeKind {
        /** Value starts with the pattern. */
        STARTS_WITH,
        /** Value ends with the pattern. */
        ENDS_WITH,
        /** Value contains the pattern. */
        CONTAINS
    }

    /**
     * String pattern match; the value is matched literally.
     * @param column matched column
     * @param kind match kind
     * @param value plain string, not a LIKE pattern
     */
    record Like(Column column, LikeKind kind, String value) implements Expr {
    }

    /**
     * Constant boolean predicate.
     * @param value constant value
     */
    record Constant(boolean value) implements Expr {
    }

    /**
     * Binary arithmetic on numeric values.
     * @param op operator: "+", "-" or "*"
     * @param left left operand
     * @param right right operand
     */
    record Arithmetic(String op, Expr left, Expr right) implements Expr {
        /**
         * Validates the operator.
         * @param op operator
         * @param left left operand
         * @param right right operand
         */
        public Arithmetic {
            if (!"+".equals(op) && !"-".equals(op) && !"*".equals(op)) {
                throw new IllegalArgumentException("Unsupported arithmetic operator: " + op);
            }
        }
    }

    /**
     * Cast to a server SQL type.
     * @param child cast expression
     * @param sqlType target type in server SQL, for example "decimal(38,2)"
     */
    record Cast(Expr child, String sqlType) implements Expr {
    }

    /**
     * Creates a literal or throws for unsupported values.
     * @param value literal value
     * @return literal
     * @throws IllegalArgumentException if the value has no exact SQL form
     */
    static Literal literal(Object value) {
        return Literal.of(value).orElseThrow(() ->
                new IllegalArgumentException("Unsupported literal: " + value));
    }

    /**
     * Creates a decimal cast with explicit precision and scale.
     * @param child cast expression
     * @param precision decimal precision
     * @param scale decimal scale
     * @return cast expression
     */
    static Cast castDecimal(Expr child, int precision, int scale) {
        return new Cast(child, String.format("decimal(%d,%d)", precision, scale));
    }
}
