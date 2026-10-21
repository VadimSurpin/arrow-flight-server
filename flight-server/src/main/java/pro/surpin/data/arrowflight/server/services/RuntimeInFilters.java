package pro.surpin.data.arrowflight.server.services;

import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.StringJoiner;

/**
 * Runtime IN-filters a Flight client built while its query was running, such as the keys of a join's build side,
 * and the rule for applying them to a scan.
 *
 * <p>A filter only ever removes rows the client would discard anyway, so ignoring one is always safe.
 * The block is little-endian: magic, filter count, then per filter the column name (length, UTF-8 bytes) and its
 * values (count, int64 each).
 */
public final class RuntimeInFilters {

    /** Block marker, "ARF1". */
    static final int MAGIC = 0x41524631;
    /** Most filters one scan can carry. */
    static final int MAX_FILTERS = 16;
    /** Longest column name accepted. */
    static final int MAX_NAME_BYTES = 256;
    /** Most values accepted across all filters of one block. */
    static final int MAX_VALUES = 2_000_000;

    private RuntimeInFilters() {
    }

    /**
     * One {@code column IN (values)} filter.
     *
     * @param column name of the scan's output column
     * @param values the accepted key values
     */
    public record Filter(String column, long[] values) {
    }

    /**
     * Serialises filters into a block.
     *
     * @param filters filters to send
     * @return the block bytes
     */
    public static byte[] encode(List<Filter> filters) {
        int size = Integer.BYTES * 2;
        for (Filter filter : filters) {
            size += Integer.BYTES + filter.column().getBytes(StandardCharsets.UTF_8).length
                    + Integer.BYTES + Long.BYTES * filter.values().length;
        }
        ByteBuffer block = ByteBuffer.allocate(size).order(ByteOrder.LITTLE_ENDIAN);
        block.putInt(MAGIC).putInt(filters.size());
        for (Filter filter : filters) {
            byte[] name = filter.column().getBytes(StandardCharsets.UTF_8);
            block.putInt(name.length).put(name).putInt(filter.values().length);
            for (long value : filter.values()) {
                block.putLong(value);
            }
        }
        return block.array();
    }

    /**
     * Parses a block, rejecting anything malformed or larger than the limits.
     *
     * @param bytes block bytes
     * @return the filters in block order
     * @throws IllegalArgumentException when the block is not valid
     */
    public static List<Filter> decode(byte[] bytes) {
        ByteBuffer block = ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN);
        try {
            if (block.getInt() != MAGIC) {
                throw new IllegalArgumentException("Unsupported runtime filter block");
            }
            int count = block.getInt();
            if (count < 0 || count > MAX_FILTERS) {
                throw new IllegalArgumentException("Invalid runtime filter count");
            }
            List<Filter> filters = new ArrayList<>(count);
            long total = 0;
            for (int i = 0; i < count; i++) {
                int nameLength = block.getInt();
                if (nameLength < 0 || nameLength > MAX_NAME_BYTES || nameLength > block.remaining()) {
                    throw new IllegalArgumentException("Invalid runtime filter column name");
                }
                byte[] name = new byte[nameLength];
                block.get(name);
                int valueCount = block.getInt();
                total += valueCount;
                if (valueCount < 0 || total > MAX_VALUES || (long) valueCount * Long.BYTES > block.remaining()) {
                    throw new IllegalArgumentException("Invalid runtime filter value count");
                }
                long[] values = new long[valueCount];
                block.asLongBuffer().get(values);
                block.position(block.position() + valueCount * Long.BYTES);
                filters.add(new Filter(new String(name, StandardCharsets.UTF_8), values));
            }
            if (block.hasRemaining()) {
                throw new IllegalArgumentException("Unexpected runtime filter data");
            }
            return filters;
        } catch (BufferUnderflowException e) {
            throw new IllegalArgumentException("Truncated runtime filter block", e);
        }
    }

    /**
     * Restricts a scan query to the rows the filters accept. Filters on columns the query does not return
     * are ignored, and so is every filter when the query's output columns are not known.
     *
     * @param sql the scan's DuckDB SQL
     * @param outputColumns the columns the query returns
     * @param filters the filters to apply
     * @return {@code sql}, wrapped in a filtering query when at least one filter applies
     */
    public static String apply(String sql, Collection<String> outputColumns, List<Filter> filters) {
        StringJoiner conditions = new StringJoiner(" AND ");
        for (Filter filter : filters) {
            String column = outputColumn(outputColumns, filter.column());
            if (column == null) {
                continue;
            }
            if (filter.values().length == 0) {
                return "SELECT * FROM (" + sql + ") AS rf WHERE FALSE";
            }
            StringJoiner values = new StringJoiner(",");
            for (long value : filter.values()) {
                values.add(Long.toString(value));
            }
            conditions.add("\"" + column.replace("\"", "\"\"") + "\" IN (" + values + ")");
        }
        if (conditions.length() == 0) {
            return sql;
        }
        return "SELECT * FROM (" + sql + ") AS rf WHERE " + conditions;
    }

    private static String outputColumn(Collection<String> outputColumns, String wanted) {
        String lower = wanted.toLowerCase(Locale.ROOT);
        for (String column : outputColumns) {
            if (column.toLowerCase(Locale.ROOT).equals(lower)) {
                return column;
            }
        }
        return null;
    }
}
