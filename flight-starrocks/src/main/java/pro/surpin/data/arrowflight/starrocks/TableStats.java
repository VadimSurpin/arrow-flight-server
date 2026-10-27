package pro.surpin.data.arrowflight.starrocks;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.List;
import java.util.Map;

/**
 * Statistics the Flight server reports for a table: the row count and, per requested column, bounds, null count,
 * size and an estimated number of distinct values (null while the server is still computing it).
 *
 * @param rows row count
 * @param columns statistics by column name
 */
public record TableStats(long rows, Map<String, ColumnStats> columns) {
    private static final ObjectMapper JSON = new ObjectMapper()
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    /**
     * Statistics of one column.
     *
     * @param min smallest value as a number (dates as epoch days, decimals by value), or null when unknown
     * @param max largest value, or null when unknown
     * @param nulls number of nulls, or -1 when unknown
     * @param bytes uncompressed size of the column
     * @param ndv estimated distinct values, or null while pending
     * @param separableGroups number of groups of files that share no value of the column (the server's Parquet
     *        footers prove it); zero when unknown, one when nothing can be separated
     */
    public record ColumnStats(Double min, Double max, long nulls, long bytes, Long ndv, int separableGroups) {
        /**
         * Statistics without separation information.
         *
         * @param min smallest value, or null
         * @param max largest value, or null
         * @param nulls number of nulls, or -1
         * @param bytes uncompressed size
         * @param ndv distinct values, or null
         */
        public ColumnStats(Double min, Double max, long nulls, long bytes, Long ndv) {
            this(min, max, nulls, bytes, ndv, 0);
        }
    }

    /**
     * Builds the request body of the statistics action.
     *
     * @param schema schema name
     * @param table table name
     * @param columns columns of interest
     * @return JSON request
     */
    static String request(String schema, String table, List<String> columns) {
        try {
            Map<String, Object> body = new java.util.LinkedHashMap<>();
            body.put("schema", schema);
            body.put("table", table);
            body.put("columns", columns);
            return JSON.writeValueAsString(body);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException(e);
        }
    }

    /**
     * Parses the server's answer.
     *
     * @param json JSON response
     * @return statistics
     */
    static TableStats parse(String json) {
        try {
            return JSON.readValue(json, TableStats.class);
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException("Malformed table statistics: " + e.getOriginalMessage(), e);
        }
    }
}
