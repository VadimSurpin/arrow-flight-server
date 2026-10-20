package pro.surpin.data.arrowflight.starrocks;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

/**
 * A scalar expression the frontend may push into a Flight SQL aggregate; renders to restricted SQL. Only columns,
 * constants, arithmetic, casts and CASE are expressible, so the server never receives free-form SQL.
 */
public sealed interface ScanExpr {
    /** Arithmetic operators. */
    enum Op {
        ADD("+"), SUB("-"), MUL("*");

        private final String sql;

        Op(String sql) {
            this.sql = sql;
        }

        String sql() {
            return sql;
        }
    }

    /**
     * A table column.
     *
     * @param name column name
     */
    record Column(String name) implements ScanExpr {
        public Column {
            identifier(name);
        }

        @Override
        public String toSql() {
            return name;
        }
    }

    /**
     * A constant.
     *
     * @param value the literal
     */
    record Constant(ScanPredicate.Literal value) implements ScanExpr {
        public Constant {
            if (value == null) {
                throw new IllegalArgumentException("A constant needs a value");
            }
        }

        @Override
        public String toSql() {
            return value.toSql();
        }
    }

    /**
     * {@code (left op right)}.
     *
     * @param op operator
     * @param left left operand
     * @param right right operand
     */
    record Arithmetic(Op op, ScanExpr left, ScanExpr right) implements ScanExpr {
        public Arithmetic {
            if (op == null || left == null || right == null) {
                throw new IllegalArgumentException("Arithmetic needs an operator and two operands");
            }
        }

        @Override
        public String toSql() {
            return "(" + left.toSql() + " " + op.sql() + " " + right.toSql() + ")";
        }
    }

    /**
     * {@code CAST(input AS type)} for the few target types the pushdown relies on.
     *
     * @param input operand
     * @param type SQL type, one of DOUBLE, BIGINT or DECIMAL(p,s)
     */
    record Cast(ScanExpr input, String type) implements ScanExpr {
        private static final Pattern DECIMAL = Pattern.compile("DECIMAL\\((\\d{1,2}),(\\d{1,2})\\)");

        public Cast {
            if (input == null || type == null) {
                throw new IllegalArgumentException("A cast needs an operand and a type");
            }
            if (!type.equals("DOUBLE") && !type.equals("BIGINT")) {
                var m = DECIMAL.matcher(type);
                if (!m.matches() || !validDecimal(Integer.parseInt(m.group(1)), Integer.parseInt(m.group(2)))) {
                    throw new IllegalArgumentException("Unsupported cast type: " + type);
                }
            }
        }

        /**
         * Casts to DOUBLE.
         *
         * @param input operand
         * @return cast expression
         */
        public static Cast toDouble(ScanExpr input) {
            return new Cast(input, "DOUBLE");
        }

        /**
         * Casts to BIGINT.
         *
         * @param input operand
         * @return cast expression
         */
        public static Cast toBigint(ScanExpr input) {
            return new Cast(input, "BIGINT");
        }

        /**
         * Casts to an exact decimal.
         *
         * @param input operand
         * @param precision total digits, 1 to 38
         * @param scale digits after the point, 0 to precision
         * @return cast expression
         */
        public static Cast toDecimal(ScanExpr input, int precision, int scale) {
            if (!validDecimal(precision, scale)) {
                throw new IllegalArgumentException("Unsupported decimal type: DECIMAL(" + precision + "," + scale + ")");
            }
            return new Cast(input, "DECIMAL(" + precision + "," + scale + ")");
        }

        private static boolean validDecimal(int precision, int scale) {
            return precision >= 1 && precision <= 38 && scale >= 0 && scale <= precision;
        }

        @Override
        public String toSql() {
            return "CAST(" + input.toSql() + " AS " + type + ")";
        }
    }

    /**
     * {@code CASE WHEN condition THEN then ELSE otherwise END}.
     *
     * @param condition filter condition
     * @param then value when the condition holds
     * @param otherwise value otherwise
     */
    record Case(ScanPredicate condition, ScanExpr then, ScanExpr otherwise) implements ScanExpr {
        public Case {
            if (condition == null || then == null || otherwise == null) {
                throw new IllegalArgumentException("CASE needs a condition and both branches");
            }
        }

        @Override
        public String toSql() {
            return "CASE WHEN " + condition.toSql() + " THEN " + then.toSql() + " ELSE " + otherwise.toSql() + " END";
        }
    }

    /**
     * Renders this expression as SQL.
     *
     * @return SQL scalar expression
     */
    String toSql();

    /**
     * Columns this expression reads, in first-use order without duplicates.
     *
     * @return column names
     */
    default List<String> columns() {
        List<String> found = new ArrayList<>();
        collect(this, found);
        return found;
    }

    private static void collect(ScanExpr expr, List<String> found) {
        if (expr instanceof Column c) {
            add(found, c.name());
        } else if (expr instanceof Arithmetic a) {
            collect(a.left(), found);
            collect(a.right(), found);
        } else if (expr instanceof Cast c) {
            collect(c.input(), found);
        } else if (expr instanceof Case c) {
            ScanPredicate.columnsOf(c.condition()).forEach(name -> add(found, name));
            collect(c.then(), found);
            collect(c.otherwise(), found);
        }
    }

    private static void add(List<String> found, String name) {
        if (!found.contains(name)) {
            found.add(name);
        }
    }

    private static void identifier(String name) {
        if (name == null || !ScanPredicate.IDENTIFIER.matcher(name).matches()) {
            throw new IllegalArgumentException("Unsupported SQL identifier: " + name);
        }
    }
}
