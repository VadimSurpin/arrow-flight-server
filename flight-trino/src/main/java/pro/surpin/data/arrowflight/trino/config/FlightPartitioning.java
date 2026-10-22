package pro.surpin.data.arrowflight.trino.config;

import pro.surpin.data.arrowflight.client.expr.SqlRenderer;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Describes optional read partitions for one remote table.
 * @param table qualified remote table name, or empty when disabled
 * @param column partition column, or empty for explicit predicates
 * @param size number of generated partitions
 * @param hashFunction remote SQL hash function
 * @param lower lower range bound used to choose split points
 * @param upper upper range bound used to choose split points
 * @param predicates explicit SQL predicates, which must cover the table without overlap
 */
public record FlightPartitioning(String table, String column, int size, String hashFunction,
                                 String lower, String upper, List<String> predicates) {
    static final Set<String> PROPERTIES = Set.of("flight.partition.table", "flight.partition.by-column",
            "flight.partition.size", "flight.partition.hash-function", "flight.partition.lower-bound",
            "flight.partition.upper-bound", "flight.partition.predicates");
    public static final FlightPartitioning NONE = new FlightPartitioning("", "", 1, "hash", null, null, List.of());

    /**
     * Copies explicit predicates.
     * @param table qualified table
     * @param column partition column
     * @param size partition count
     * @param hashFunction hash function
     * @param lower lower bound
     * @param upper upper bound
     * @param predicates explicit predicates
     */
    public FlightPartitioning {
        predicates = List.copyOf(predicates);
    }

    /**
     * Parses catalog partition settings.
     * @param props catalog properties
     * @return partition settings
     * @throws IllegalArgumentException if settings are incomplete or invalid
     */
    public static FlightPartitioning fromProperties(Map<String, String> props) {
        if (PROPERTIES.stream().noneMatch(props::containsKey)) {
            return NONE;
        }
        String table = props.getOrDefault("flight.partition.table", "").trim();
        String column = props.getOrDefault("flight.partition.by-column", "").trim();
        String hash = props.getOrDefault("flight.partition.hash-function", "hash").trim();
        int size = Integer.parseInt(props.getOrDefault("flight.partition.size", "6").trim());
        String lower = props.get("flight.partition.lower-bound");
        String upper = props.get("flight.partition.upper-bound");
        String explicit = props.get("flight.partition.predicates");
        List<String> predicates = explicit == null ? List.of()
                : Arrays.stream(explicit.split(";", -1)).map(String::trim).toList();
        if (table.isEmpty() || !table.contains(".")) {
            throw new IllegalArgumentException("flight.partition.table must specify schema.table");
        }
        if (size < 1 || size > 10_000 || predicates.size() > 10_000) {
            throw new IllegalArgumentException("Partition count must be between 1 and 10000");
        }
        if (!hash.matches("[A-Za-z_][A-Za-z0-9_]*(\\.[A-Za-z_][A-Za-z0-9_]*)*")) {
            throw new IllegalArgumentException("Invalid flight.partition.hash-function");
        }
        if ((lower == null) != (upper == null) || (lower != null && column.isEmpty())) {
            throw new IllegalArgumentException("Range partitioning requires a column and both bounds");
        }
        if (column.isEmpty() == predicates.isEmpty() || predicates.stream().anyMatch(String::isEmpty)) {
            throw new IllegalArgumentException("Set either flight.partition.by-column or nonempty predicates");
        }
        return new FlightPartitioning(table, column, size, hash, lower, upper, predicates);
    }

    /**
     * Checks whether partitioning applies to a remote table.
     * @param schema remote schema
     * @param name remote table
     * @return whether the table matches
     */
    public boolean appliesTo(String schema, String name) {
        return table.equals(schema + "." + name);
    }

    /**
     * Generates disjoint predicates preserving nulls and values outside the supplied bounds.
     * @param schema full table schema, including unprojected columns
     * @param quote identifier quote
     * @return partition predicates
     * @throws IllegalArgumentException if the column or range is invalid
     */
    public List<String> predicates(Schema schema, String quote) {
        if (!predicates.isEmpty()) {
            return predicates;
        }
        Field field = schema.getFields().stream().filter(f -> f.getName().equals(column)).findFirst()
                .orElseThrow(() -> new IllegalArgumentException("Unknown partition column: " + column));
        String name = new SqlRenderer(quote).quoteIdentifier(field.getName());
        if (size == 1) {
            return List.of("true");
        }
        if (lower != null) {
            return ranges(field.getType(), name);
        }
        String bucket = "((" + hashFunction + "(" + name + ") % " + size + " + " + size + ") % " + size + ")";
        List<String> result = new ArrayList<>();
        for (int i = 0; i < size; i++) {
            result.add(i == 0 ? "(" + name + " IS NULL OR (" + name + " IS NOT NULL AND " + bucket + " = 0))"
                    : "(" + name + " IS NOT NULL AND " + bucket + " = " + i + ")");
        }
        return List.copyOf(result);
    }

    /**
     * Builds ranges with unbounded first and last partitions.
     * @param type partition field type
     * @param name quoted column
     * @return predicates
     * @throws IllegalArgumentException if bounds are invalid or the type is unsupported
     */
    private List<String> ranges(ArrowType type, String name) {
        BigDecimal start = bound(type, lower);
        BigDecimal end = bound(type, upper);
        if (start.compareTo(end) >= 0) {
            throw new IllegalArgumentException("Partition lower bound must be less than upper bound");
        }
        List<String> cuts = new ArrayList<>();
        for (int i = 1; i < size; i++) {
            BigDecimal point = start.add(end.subtract(start).multiply(BigDecimal.valueOf(i))
                    .divide(BigDecimal.valueOf(size), 18, RoundingMode.FLOOR));
            String cut = literal(type, point);
            if (cuts.isEmpty() || !cut.equals(cuts.getLast())) {
                cuts.add(cut);
            }
        }
        List<String> result = new ArrayList<>();
        result.add("(" + name + " IS NULL OR " + name + " < " + cuts.getFirst() + ")");
        for (int i = 1; i < cuts.size(); i++) {
            result.add("(" + name + " >= " + cuts.get(i - 1) + " AND " + name + " < " + cuts.get(i) + ")");
        }
        result.add(name + " >= " + cuts.getLast());
        return List.copyOf(result);
    }

    /**
     * Parses a numeric or ISO temporal range bound.
     * @param type Arrow column type
     * @param text bound text
     * @return exact numeric bound
     * @throws IllegalArgumentException if the bound or type is unsupported
     */
    private static BigDecimal bound(ArrowType type, String text) {
        return switch (type) {
            case ArrowType.Int ignored -> new BigDecimal(text.trim());
            case ArrowType.FloatingPoint ignored -> new BigDecimal(text.trim());
            case ArrowType.Decimal ignored -> new BigDecimal(text.trim());
            case ArrowType.Date ignored -> BigDecimal.valueOf(LocalDate.parse(text.trim()).toEpochDay());
            case ArrowType.Timestamp timestamp -> {
                if (timestamp.getTimezone() != null) {
                    throw new IllegalArgumentException("Use explicit predicates for zoned timestamp partitions");
                }
                LocalDateTime value = LocalDateTime.parse(text.trim().replace(' ', 'T'));
                yield BigDecimal.valueOf(value.toEpochSecond(ZoneOffset.UTC)).add(BigDecimal.valueOf(value.getNano(), 9));
            }
            default -> throw new IllegalArgumentException("Unsupported range partition type: " + type);
        };
    }

    /**
     * Renders a numeric or temporal split point.
     * @param type Arrow column type
     * @param point split point
     * @return SQL literal
     */
    private static String literal(ArrowType type, BigDecimal point) {
        if (type instanceof ArrowType.Date) {
            return "DATE '" + LocalDate.ofEpochDay(point.setScale(0, RoundingMode.FLOOR).longValueExact()) + "'";
        }
        if (type instanceof ArrowType.Timestamp) {
            long seconds = point.setScale(0, RoundingMode.FLOOR).longValueExact();
            int nanos = point.subtract(BigDecimal.valueOf(seconds)).movePointRight(9).intValue();
            return "TIMESTAMP '" + LocalDateTime.ofEpochSecond(seconds, nanos, ZoneOffset.UTC)
                    .toString().replace('T', ' ') + "'";
        }
        return (type instanceof ArrowType.Int ? point.setScale(0, RoundingMode.FLOOR) : point)
                .stripTrailingZeros().toPlainString();
    }
}
