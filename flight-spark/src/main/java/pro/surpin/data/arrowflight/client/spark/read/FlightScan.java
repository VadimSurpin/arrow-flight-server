package pro.surpin.data.arrowflight.client.spark.read;

import pro.surpin.data.arrowflight.client.Configuration;
import pro.surpin.data.arrowflight.client.model.Table;
import org.apache.spark.sql.connector.read.Batch;
import org.apache.spark.sql.connector.read.Statistics;
import org.apache.spark.sql.connector.read.SupportsReportStatistics;
import org.apache.spark.sql.connector.read.SupportsRuntimeV2Filtering;
import org.apache.spark.sql.connector.expressions.Expression;
import org.apache.spark.sql.connector.expressions.filter.Predicate;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.connector.expressions.Expressions;
import org.apache.spark.sql.connector.expressions.NamedReference;
import org.apache.spark.sql.connector.read.colstats.ColumnStatistics;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.OptionalLong;

/**
 * Describes the data-structure of FlightScan
 */
public class FlightScan implements SupportsReportStatistics, SupportsRuntimeV2Filtering, Serializable {
    private static final Logger LOGGER = LoggerFactory.getLogger(FlightScan.class);
    private static final int MAX_RUNTIME_KEYS = 100_000;

    private final Configuration configuration;
    private final Table table;
    private final boolean aggregated;
    // Filters pushed into the scan, whose selectivity shrinks the size estimate.
    private final org.apache.spark.sql.sources.Filter[] filters;
    private final Predicate[] predicates;
    // Re-plans the scan with extra WHERE parts; null when runtime filtering is not possible. Driver-side only.
    private final transient Replanner replanner;

    /**
     * Construct a FligthScan
     * @param configuration - the configuration of remote flight service
     * @param table - the table object
     */
    public FlightScan(Configuration configuration, Table table) {
        this(configuration, table, false);
    }

    /**
     * Construct a FligthScan
     * @param configuration - the configuration of remote flight service
     * @param table - the table object
     * @param aggregated - true when an aggregation was pushed, so the output columns are not table columns
     */
    public FlightScan(Configuration configuration, Table table, boolean aggregated) {
        this(configuration, table, aggregated, null);
    }

    /**
     * Construct a FligthScan that accepts runtime filters
     * @param configuration - the configuration of remote flight service
     * @param table - the table object
     * @param aggregated - true when an aggregation was pushed, so the output columns are not table columns
     * @param replanner - plans the scan again with extra filters, or null to refuse runtime filters
     */
    public FlightScan(Configuration configuration, Table table, boolean aggregated, Replanner replanner) {
        this(configuration, table, aggregated, replanner, new org.apache.spark.sql.sources.Filter[0],
                new Predicate[0]);
    }

    /**
     * Construct a FligthScan that knows which filters were pushed into it
     * @param configuration - the configuration of remote flight service
     * @param table - the table object
     * @param aggregated - true when an aggregation was pushed, so the output columns are not table columns
     * @param replanner - plans the scan again with extra filters, or null to refuse runtime filters
     * @param filters - pushed V1 filters, used to estimate how many rows the scan returns
     * @param predicates - pushed V2 predicates, used the same way
     */
    public FlightScan(Configuration configuration, Table table, boolean aggregated, Replanner replanner,
            org.apache.spark.sql.sources.Filter[] filters, Predicate[] predicates) {
        LOGGER.debug("{}()", this.getClass().getName());
        this.filters = filters;
        this.predicates = predicates;

        this.configuration = configuration;
        this.table = table;
        this.aggregated = aggregated;
        this.replanner = replanner;
    }

    /** Plans the scan again on the Flight server with extra filter conditions. */
    @FunctionalInterface
    public interface Replanner {
        /**
         * Applies the conditions on top of the filters the scan already carries.
         *
         * @param extraConditions rendered WHERE conditions
         */
        void replan(List<String> extraConditions);
    }

    /**
     * The integer key columns Spark may prune with the join keys of a small side.
     *
     * @return key columns, none for an aggregated scan
     */
    @Override
    public NamedReference[] filterAttributes() {
        StructType schema = readSchema();
        if (this.replanner == null || this.aggregated || schema == null) {
            return new NamedReference[0];
        }
        return Arrays.stream(schema.fields())
                .filter(f -> isKey(f.dataType()))
                .map(f -> Expressions.column(f.name()))
                .toArray(NamedReference[]::new);
    }

    private static boolean isKey(DataType type) {
        return DataTypes.LongType.equals(type) || DataTypes.IntegerType.equals(type);
    }

    /**
     * Narrows the scan to the join keys Spark found at run time. Spark still performs the join, so the server only
     * has to return a superset; a filter with more keys than {@value #MAX_RUNTIME_KEYS} is not worth sending.
     *
     * @param predicates IN predicates on key columns
     */
    @Override
    public void filter(Predicate[] predicates) {
        if (this.replanner == null) {
            return;
        }
        List<String> conditions = new ArrayList<>();
        for (Predicate predicate : predicates) {
            Expression[] children = predicate.children();
            if ("IN".equals(predicate.name()) && children.length > 1 && children.length <= MAX_RUNTIME_KEYS + 1
                    && this.table.canPushPredicate(predicate)) {
                conditions.add(this.table.toWhereClause(predicate));
            }
        }
        if (!conditions.isEmpty()) {
            LOGGER.debug("{}.filter(): {} runtime filters", this.getClass().getName(), conditions.size());
            this.replanner.replan(conditions);
        }
    }

    /**
     * Get the schema of the scan
     * @return - the scan for the scan
     */
    @Override
    public StructType readSchema() {
        return this.table.getSparkSchema();
    }

    /**
     * The description of the scan
     * @return - description
     */
    @Override
    public String description() {
        return this.table.getQueryStatement();
    }

    /**
     * Reports Parquet input estimates transported in the standard FlightInfo fields.
     *
     * @return Spark scan statistics
     */
    @Override
    public Statistics estimateStatistics() {
        long tableBytes = this.table.getEstimatedBytes();
        long tableRows = this.table.getEstimatedRows();
        StructType schema = readSchema();
        Map<NamedReference, ColumnStatistics> columns = Collections.emptyMap();
        double selectivity = 1.0;
        if (!this.aggregated && schema != null && this.table.getName() != null) {
            Set<String> names = new LinkedHashSet<>(Arrays.stream(schema.fields()).map(f -> f.name()).toList());
            names.addAll(FilterSelectivity.columns(this.filters, this.predicates));
            Optional<FlightColumnStatistics.Answer> answer =
                    FlightColumnStatistics.fetch(this.configuration, this.table.getName(), names);
            if (answer.isPresent() && answer.get().columns() != null) {
                columns = FlightColumnStatistics.convert(answer.get(), schema);
                Map<String, FlightColumnStatistics.Column> byName = new HashMap<>();
                answer.get().columns().forEach((name, column) -> byName.put(name, column));
                long rows = tableRows >= 0 ? tableRows : answer.get().rows();
                selectivity = FilterSelectivity.of(this.filters, this.predicates, byName, rows);
            } else {
                selectivity = FilterSelectivity.of(this.filters, this.predicates, Map.of(), Math.max(tableRows, 1));
            }
        }
        long bytes = tableBytes >= 0 ? Math.max(1, Math.round(tableBytes * selectivity)) : tableBytes;
        long rows = tableRows >= 0 ? Math.max(1, Math.round(tableRows * selectivity)) : tableRows;
        Map<NamedReference, ColumnStatistics> reportedColumns = columns;
        return new Statistics() {
            @Override
            public Map<NamedReference, ColumnStatistics> columnStats() {
                return reportedColumns;
            }

            @Override
            public OptionalLong sizeInBytes() {
                return bytes >= 0 ? OptionalLong.of(bytes) : OptionalLong.empty();
            }

            @Override
            public OptionalLong numRows() {
                return rows >= 0 ? OptionalLong.of(rows) : OptionalLong.empty();
            }
        };
    }

    /**
     * Translate the scan to batch
     * @return - the batch desribes the scan
     */
    @Override
    public Batch toBatch() {
        LOGGER.debug("{}.toBatch()", this.getClass().getName());
        return new FlightBatch(this.configuration, this.table);
    }
}
