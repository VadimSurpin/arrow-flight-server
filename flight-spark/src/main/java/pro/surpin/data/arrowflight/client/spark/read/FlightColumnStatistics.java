package pro.surpin.data.arrowflight.client.spark.read;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import org.apache.arrow.flight.Action;
import org.apache.arrow.flight.CallStatus;
import org.apache.arrow.flight.FlightRuntimeException;
import org.apache.arrow.flight.Result;
import org.apache.spark.sql.connector.expressions.Expressions;
import org.apache.spark.sql.connector.expressions.NamedReference;
import org.apache.spark.sql.connector.read.colstats.ColumnStatistics;
import org.apache.spark.sql.types.ByteType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DateType;
import org.apache.spark.sql.types.DecimalType;
import org.apache.spark.sql.types.DoubleType;
import org.apache.spark.sql.types.FloatType;
import org.apache.spark.sql.types.IntegerType;
import org.apache.spark.sql.types.LongType;
import org.apache.spark.sql.types.ShortType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pro.surpin.data.arrowflight.client.Client;
import pro.surpin.data.arrowflight.client.Configuration;

/**
 * Column statistics the Flight server offers through its {@code arrowflight.table_statistics} action: the number of
 * distinct values, bounds and nulls of each column. Spark's cost-based optimizer uses them to order joins and to pick
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
     * @param min smallest value as a number (dates as epoch days), or null when unknown
     * @param max largest value, or null when unknown
     * @param nulls number of nulls, or -1 when unknown
     * @param ndv estimated distinct values, or null while the server still computes them
     * @param separableGroups number of groups of files that share no value of the column, zero when unknown
     */
    record Column(Double min, Double max, long nulls, Long ndv, int separableGroups) { }

    /**
     * Fetches the statistics of the given columns.
     *
     * @param configuration connection configuration
     * @param source table as {@code schema.table}
     * @param columns names of the columns of interest
     * @return the server's answer; empty when the server offers none
     */
    static Optional<Answer> fetch(Configuration configuration, String source, Collection<String> columns) {
        int dot = source.indexOf('.');
        if (dot <= 0 || dot == source.length() - 1 || source.indexOf('(') >= 0 || source.indexOf(' ') >= 0
                || columns.isEmpty()) {
            return Optional.empty();
        }
        try {
            Client client = Client.getOrCreate(configuration);
            Map<String, Object> request = new LinkedHashMap<>();
            request.put("schema", source.substring(0, dot));
            request.put("table", source.substring(dot + 1));
            request.put("columns", List.copyOf(columns));
            Iterator<Result> results = client.getFlightClient().doAction(
                    new Action("arrowflight.table_statistics",
                            JSON.writeValueAsString(request).getBytes(StandardCharsets.UTF_8)),
                    client.callOptions());
            if (!results.hasNext()) {
                return Optional.empty();
            }
            return Optional.of(parse(new String(results.next().getBody(), StandardCharsets.UTF_8)));
        } catch (FlightRuntimeException e) {
            if (e.status().code() != CallStatus.UNIMPLEMENTED.code() && e.status().code() != CallStatus.NOT_FOUND.code()) {
                LOGGER.warn("Table statistics of {} unavailable: {}", source, e.getMessage());
            }
            return Optional.empty();
        } catch (JsonProcessingException | RuntimeException e) {
            LOGGER.warn("Table statistics of {} unavailable: {}", source, e.toString());
            return Optional.empty();
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
     * Converts the answer to the form Spark reads. A column is reported only with its complete set: distinct count,
     * minimum and maximum. Spark's join estimation reads a missing bound as an empty value range and then estimates
     * that the join returns no rows, which makes it broadcast tables of any size.
     *
     * @param answer server statistics
     * @param schema Spark types of the columns
     * @return statistics by column reference
     */
    static Map<NamedReference, ColumnStatistics> convert(Answer answer, StructType schema) {
        Map<NamedReference, ColumnStatistics> out = new LinkedHashMap<>();
        if (answer.columns() == null) {
            return out;
        }
        answer.columns().forEach((name, column) -> {
            Optional<StructField> field = Arrays.stream(schema.fields())
                    .filter(f -> f.name().equalsIgnoreCase(name)).findFirst();
            if (field.isEmpty() || column.ndv() == null || column.ndv() <= 0 || column.min() == null
                    || column.max() == null) {
                return;
            }
            Optional<Object> min = bound(column.min(), field.get().dataType());
            Optional<Object> max = bound(column.max(), field.get().dataType());
            if (min.isEmpty() || max.isEmpty()) {
                return;
            }
            OptionalLong ndv = OptionalLong.of(column.ndv());
            OptionalLong nulls = column.nulls() >= 0 ? OptionalLong.of(column.nulls()) : OptionalLong.empty();
            out.put(Expressions.column(field.get().name()), new ColumnStatistics() {
                @Override
                public OptionalLong distinctCount() {
                    return ndv;
                }

                @Override
                public Optional<Object> min() {
                    return min;
                }

                @Override
                public Optional<Object> max() {
                    return max;
                }

                @Override
                public OptionalLong nullCount() {
                    return nulls;
                }
            });
        });
        return out;
    }

    private static Optional<Object> bound(double value, DataType type) {
        if (type instanceof LongType) {
            return Optional.of((long) value);
        } else if (type instanceof IntegerType) {
            return Optional.of((int) value);
        } else if (type instanceof ShortType) {
            return Optional.of((short) value);
        } else if (type instanceof ByteType) {
            return Optional.of((byte) value);
        } else if (type instanceof DoubleType) {
            return Optional.of(value);
        } else if (type instanceof FloatType) {
            return Optional.of((float) value);
        } else if (type instanceof DecimalType) {
            return Optional.of(java.math.BigDecimal.valueOf(value));
        } else if (type instanceof DateType) {
            return Optional.of(java.time.LocalDate.ofEpochDay((long) value));
        }
        return Optional.empty();
    }
}
