package pro.surpin.data.arrowflight.client.spark.read;

import java.time.LocalDate;
import java.util.LinkedHashSet;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import org.apache.spark.sql.connector.expressions.Expression;
import org.apache.spark.sql.connector.expressions.Literal;
import org.apache.spark.sql.connector.expressions.NamedReference;
import org.apache.spark.sql.connector.expressions.filter.Predicate;
import org.apache.spark.sql.sources.And;
import org.apache.spark.sql.sources.EqualNullSafe;
import org.apache.spark.sql.sources.EqualTo;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.sources.GreaterThan;
import org.apache.spark.sql.sources.GreaterThanOrEqual;
import org.apache.spark.sql.sources.In;
import org.apache.spark.sql.sources.IsNotNull;
import org.apache.spark.sql.sources.IsNull;
import org.apache.spark.sql.sources.LessThan;
import org.apache.spark.sql.sources.LessThanOrEqual;
import org.apache.spark.sql.sources.Not;
import org.apache.spark.sql.sources.Or;
import org.apache.spark.sql.sources.StringContains;
import org.apache.spark.sql.sources.StringEndsWith;
import org.apache.spark.sql.sources.StringStartsWith;
import pro.surpin.data.arrowflight.client.spark.read.FlightColumnStatistics.Column;

/**
 * Estimates the share of a table's rows that survive the filters pushed into a scan.
 *
 * <p>Spark removes pushed filters from the plan, so its own filter estimation never sees them and the scan looks as big
 * as the whole table. Join reordering then joins the biggest tables first. The estimate uses distinct counts, bounds
 * and null counts of the Flight server when it has them and fixed fractions otherwise; conditions are taken as
 * independent.
 */
final class FilterSelectivity {
    private static final double PATTERN = 0.1;
    private static final double EQUALS = 0.1;
    private static final double RANGE = 1.0 / 3;

    private FilterSelectivity() {
    }

    /**
     * Estimates the fraction of rows the filters keep.
     *
     * @param filters pushed V1 filters
     * @param predicates pushed V2 predicates
     * @param stats server statistics by column name, possibly empty
     * @param rows rows in the table, used as the floor of one row
     * @return fraction in (0, 1]; 1 without filters
     */
    static double of(Filter[] filters, Predicate[] predicates, Map<String, Column> stats, long rows) {
        double selectivity = 1.0;
        for (Filter filter : filters) {
            selectivity *= of(filter, stats, rows);
        }
        for (Predicate predicate : predicates) {
            selectivity *= of(predicate, stats, rows);
        }
        return Math.min(1.0, Math.max(selectivity, 1.0 / Math.max(rows, 1)));
    }

    /**
     * Names the columns whose statistics improve the estimate.
     *
     * @param filters pushed V1 filters
     * @param predicates pushed V2 predicates
     * @return column names
     */
    static Set<String> columns(Filter[] filters, Predicate[] predicates) {
        Set<String> names = new LinkedHashSet<>();
        for (Filter filter : filters) {
            for (String reference : filter.references()) {
                names.add(reference);
            }
        }
        for (Predicate predicate : predicates) {
            for (NamedReference reference : predicate.references()) {
                names.add(String.join(".", reference.fieldNames()));
            }
        }
        return names;
    }

    private static double of(Filter filter, Map<String, Column> stats, long rows) {
        if (filter instanceof And and) {
            return of(and.left(), stats, rows) * of(and.right(), stats, rows);
        } else if (filter instanceof Or or) {
            double left = of(or.left(), stats, rows);
            double right = of(or.right(), stats, rows);
            return left + right - left * right;
        } else if (filter instanceof Not not) {
            return 1.0 - of(not.child(), stats, rows);
        } else if (filter instanceof EqualTo equal) {
            return equality(stats.get(equal.attribute()));
        } else if (filter instanceof EqualNullSafe equal) {
            return equality(stats.get(equal.attribute()));
        } else if (filter instanceof GreaterThan f) {
            return above(stats.get(f.attribute()), number(f.value()));
        } else if (filter instanceof GreaterThanOrEqual f) {
            return above(stats.get(f.attribute()), number(f.value()));
        } else if (filter instanceof LessThan f) {
            return below(stats.get(f.attribute()), number(f.value()));
        } else if (filter instanceof LessThanOrEqual f) {
            return below(stats.get(f.attribute()), number(f.value()));
        } else if (filter instanceof In in) {
            return members(stats.get(in.attribute()), in.values().length);
        } else if (filter instanceof IsNull f) {
            return nulls(stats.get(f.attribute()), rows);
        } else if (filter instanceof IsNotNull f) {
            return 1.0 - nulls(stats.get(f.attribute()), rows);
        } else if (filter instanceof StringContains || filter instanceof StringStartsWith
                || filter instanceof StringEndsWith) {
            return PATTERN;
        }
        return 1.0;
    }

    private static double of(Predicate predicate, Map<String, Column> stats, long rows) {
        Expression[] children = predicate.children();
        String name = predicate.name().toUpperCase(Locale.ROOT);
        switch (name) {
            case "AND":
                return of((Predicate) children[0], stats, rows) * of((Predicate) children[1], stats, rows);
            case "OR":
                double left = of((Predicate) children[0], stats, rows);
                double right = of((Predicate) children[1], stats, rows);
                return left + right - left * right;
            case "NOT":
                return 1.0 - of((Predicate) children[0], stats, rows);
            case "STARTS_WITH":
            case "ENDS_WITH":
            case "CONTAINS":
                return PATTERN;
            case "ALWAYS_TRUE":
            default:
                break;
        }
        Column column = children.length > 0 && children[0] instanceof NamedReference reference
                ? stats.get(String.join(".", reference.fieldNames())) : null;
        double value = children.length == 2 && children[1] instanceof Literal<?> literal ? number(literal.value())
                : Double.NaN;
        return switch (name) {
            case "=", "<=>" -> equality(column);
            case ">", ">=" -> above(column, value);
            case "<", "<=" -> below(column, value);
            case "IN" -> members(column, children.length - 1);
            case "IS_NULL" -> nulls(column, rows);
            case "IS_NOT_NULL" -> 1.0 - nulls(column, rows);
            default -> 1.0;
        };
    }

    private static double equality(Column column) {
        return column != null && column.ndv() != null && column.ndv() > 0 ? 1.0 / column.ndv() : EQUALS;
    }

    private static double members(Column column, int count) {
        double each = equality(column);
        return Math.min(1.0, count * each);
    }

    private static double above(Column column, double value) {
        if (column == null || column.min() == null || column.max() == null || Double.isNaN(value)
                || column.max() <= column.min()) {
            return RANGE;
        }
        return clamp((column.max() - value) / (column.max() - column.min()));
    }

    private static double below(Column column, double value) {
        if (column == null || column.min() == null || column.max() == null || Double.isNaN(value)
                || column.max() <= column.min()) {
            return RANGE;
        }
        return clamp((value - column.min()) / (column.max() - column.min()));
    }

    private static double nulls(Column column, long rows) {
        if (column == null || column.nulls() < 0 || rows <= 0) {
            return 0.01;
        }
        return Math.min(1.0, (double) column.nulls() / rows);
    }

    private static double clamp(double fraction) {
        return Math.min(1.0, Math.max(0.0, fraction));
    }

    /** Converts a literal of any Spark representation to a number; dates become epoch days, others NaN. */
    private static double number(Object value) {
        if (value instanceof Number number) {
            return number.doubleValue();
        } else if (value instanceof org.apache.spark.sql.types.Decimal decimal) {
            return decimal.toDouble();
        } else if (value instanceof LocalDate date) {
            return date.toEpochDay();
        } else if (value instanceof java.sql.Date date) {
            return date.toLocalDate().toEpochDay();
        }
        return Double.NaN;
    }
}
