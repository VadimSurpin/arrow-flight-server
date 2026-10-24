package pro.surpin.data.arrowflight.client.spark.read;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import org.apache.arrow.flight.Action;
import org.apache.arrow.flight.CallStatus;
import org.apache.arrow.flight.FlightRuntimeException;
import org.apache.arrow.flight.Result;
import org.apache.spark.sql.connector.expressions.Expressions;
import org.apache.spark.sql.connector.expressions.NamedReference;
import org.apache.spark.sql.connector.read.colstats.ColumnStatistics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pro.surpin.data.arrowflight.client.Client;
import pro.surpin.data.arrowflight.client.Configuration;

/**
 * Column statistics the Flight server offers through its {@code arrowflight.table_statistics} action: the number of
 * distinct values and nulls of each column. Spark's cost-based optimizer uses them to order joins and to pick
 * broadcast sides. A server without the action, or a source that is not a plain {@code schema.table}, simply yields
 * no statistics.
 */
final class FlightColumnStatistics {
    private static final Logger LOGGER = LoggerFactory.getLogger(FlightColumnStatistics.class);
    private static final ObjectMapper JSON = new ObjectMapper()
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    private FlightColumnStatistics() {
    }

    /** The part of the server's answer used here. */
    record Answer(long rows, Map<String, Column> columns) { }

    /**
     * Statistics of one column.
     *
     * @param nulls number of nulls, or -1 when unknown
     * @param ndv estimated distinct values, or null while the server still computes them
     */
    record Column(long nulls, Long ndv) { }

    /**
     * Fetches the statistics of the given columns.
     *
     * @param configuration connection configuration
     * @param source table as {@code schema.table}
     * @param columns columns of interest
     * @return column statistics by Spark reference; empty when the server offers none
     */
    static Map<NamedReference, ColumnStatistics> fetch(Configuration configuration, String source,
            List<String> columns) {
        int dot = source.indexOf('.');
        if (dot <= 0 || dot == source.length() - 1 || source.indexOf('(') >= 0 || source.indexOf(' ') >= 0
                || columns.isEmpty()) {
            return Collections.emptyMap();
        }
        try {
            Client client = Client.getOrCreate(configuration);
            Map<String, Object> request = new LinkedHashMap<>();
            request.put("schema", source.substring(0, dot));
            request.put("table", source.substring(dot + 1));
            request.put("columns", columns);
            Iterator<Result> results = client.getFlightClient().doAction(
                    new Action("arrowflight.table_statistics",
                            JSON.writeValueAsString(request).getBytes(StandardCharsets.UTF_8)),
                    client.callOptions());
            if (!results.hasNext()) {
                return Collections.emptyMap();
            }
            return convert(parse(new String(results.next().getBody(), StandardCharsets.UTF_8)));
        } catch (FlightRuntimeException e) {
            if (e.status().code() != CallStatus.UNIMPLEMENTED.code() && e.status().code() != CallStatus.NOT_FOUND.code()) {
                LOGGER.warn("Table statistics of {} unavailable: {}", source, e.getMessage());
            }
            return Collections.emptyMap();
        } catch (JsonProcessingException | RuntimeException e) {
            LOGGER.warn("Table statistics of {} unavailable: {}", source, e.toString());
            return Collections.emptyMap();
        }
    }

    /**
     * Parses the server's JSON answer.
     *
     * @param json answer body
     * @return rows and columns
     * @throws JsonProcessingException when the body is not the expected JSON
     */
    static Answer parse(String json) throws JsonProcessingException {
        return JSON.readValue(json, Answer.class);
    }

    /**
     * Converts the answer to the form Spark reads.
     *
     * @param answer server statistics
     * @return statistics by column reference, only for columns with something to say
     */
    static Map<NamedReference, ColumnStatistics> convert(Answer answer) {
        Map<NamedReference, ColumnStatistics> out = new LinkedHashMap<>();
        if (answer.columns() == null) {
            return out;
        }
        answer.columns().forEach((name, column) -> {
            OptionalLong ndv = column.ndv() != null && column.ndv() > 0 ? OptionalLong.of(column.ndv())
                    : OptionalLong.empty();
            OptionalLong nulls = column.nulls() >= 0 ? OptionalLong.of(column.nulls()) : OptionalLong.empty();
            if (ndv.isPresent() || nulls.isPresent()) {
                out.put(Expressions.column(name), new ColumnStatistics() {
                    @Override
                    public OptionalLong distinctCount() {
                        return ndv;
                    }

                    @Override
                    public OptionalLong nullCount() {
                        return nulls;
                    }
                });
            }
        });
        return out;
    }
}
