package pro.surpin.data.arrowflight.client.spark.read;

import pro.surpin.data.arrowflight.client.Configuration;
import pro.surpin.data.arrowflight.client.write.PartitionBehavior;
import pro.surpin.data.arrowflight.client.query.PushAggregation;
import pro.surpin.data.arrowflight.client.model.Table;
import pro.surpin.data.arrowflight.client.spark.FlightAqeGuard;
import pro.surpin.data.arrowflight.client.spark.SparkPushdown;
import org.apache.spark.sql.connector.expressions.aggregate.Aggregation;
import org.apache.spark.sql.connector.expressions.filter.Predicate;
import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.connector.read.ScanBuilder;
import org.apache.spark.sql.connector.read.SupportsPushDownAggregates;
import org.apache.spark.sql.connector.read.SupportsPushDownRequiredColumns;
import org.apache.spark.sql.connector.read.SupportsPushDownV2Filters;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Build flight scans which supports pushed-down filter, fields & aggregates
 */
public final class FlightScanBuilder implements ScanBuilder, SupportsPushDownV2Filters,
        SupportsPushDownRequiredColumns, SupportsPushDownAggregates {
    private static final Logger LOGGER = LoggerFactory.getLogger(FlightScanBuilder.class);

    /**
     * Spark SQL setting for keeping pushed filters in the plan: {@code true}, {@code false} (default) or
     * {@code auto}. Off by default: runtime filters of hundreds of thousands of keys became huge IN lists that
     * exhausted memory on the Flight servers' nodes in benchmarks.
     */
    public static final String RETAIN_PUSHED_FILTERS = "spark.flight.retainPushedFilters";
    /** Spark SQL setting: in {@code auto} mode, the largest table (in rows) whose filters are kept. */
    public static final String RETAIN_MAX_ROWS = "spark.flight.retainMaxRows";
    /** Spark SQL setting: in {@code auto} mode, the largest share of rows the filters may keep. */
    public static final String RETAIN_MAX_SELECTIVITY = "spark.flight.retainMaxSelectivity";

    private final Configuration configuration;
    private final Table table;
    private final PartitionBehavior partitionBehavior;

    //the pushed-down filters
    private Filter[] pdFilters = new Filter[0];
    //the pushed-down V2 predicates
    private Predicate[] pdPredicates = new Predicate[0];
    // true when the pushed predicates were returned to Spark as well
    private boolean retained;
    //the pushed-down columns
    private StructField[] pdColumns = new StructField[0];

    //pushed-down aggregation
    private PushAggregation pdAggregation = null;

    // Spark deletes the filters a source accepts, and its runtime-filter rules need a selective filter in the plan to
    // act. When retained, accepted predicates are returned to Spark as well: the server still applies them, Spark
    // applies them again to the (already filtered) rows, and a join with a big table can then be pruned at run time.
    // A kept filter also stops Spark from pushing a complete aggregation into the scan, so "auto" keeps them only for
    // tables small enough that this costs nothing.
    private String retainMode = sparkSetting(RETAIN_PUSHED_FILTERS,
            FlightAqeGuard.installed() ? "auto" : "false");
    private final long retainMaxRows = Long.parseLong(sparkSetting(RETAIN_MAX_ROWS, "50000000"));
    private final double retainMaxSelectivity = Double.parseDouble(sparkSetting(RETAIN_MAX_SELECTIVITY, "0.1"));

    /**
     * Construct a flight-scan builder
     * @param configuration - the configuration of remote flight-service
     * @param table - the table instance representing a table in remote flight-service
     * @param partitionBehavior - the partitioning behavior when loading data from remote flight service
     */
    public FlightScanBuilder(Configuration configuration, Table table, PartitionBehavior partitionBehavior) {
        LOGGER.debug("{}()", this.getClass().getName());
        this.configuration = configuration;
        this.table = table;
        this.partitionBehavior = partitionBehavior;
    }

    /**
     * Collection aggregations that will be pushed down
     * @param aggregation - the pushed aggregation
     * @return - pushed aggregation
     */
    @Override
    public boolean pushAggregation(Aggregation aggregation) {
        LOGGER.debug("{}.pushAggregation()", this.getClass().getName());

        // An aggregation with neither aggregate functions nor grouping keys has
        // nothing to push; SparkPushdown declines it so Spark keeps the plain scan.
        this.pdAggregation = SparkPushdown.translate(aggregation, this.table.getSparkSchema())
                .map(this.table.renderer()::render)
                .orElse(null);
        return this.pdAggregation != null;
    }

    /**
     * For SupportsPushDownFilters interface
     * @param filters - the pushed-down filters
     * @return - not-accepted filters
     */
    public Filter[] pushFilters(Filter[] filters) {
        LOGGER.debug("{}.pushFilters()", this.getClass().getName());

        List<Filter> pushed = new ArrayList<>();
        List<Filter> unhandled = new ArrayList<>();
        for (Filter filter : filters) {
            if (this.table.canPushFilter(filter)) {
                pushed.add(filter);
            } else {
                unhandled.add(filter);
            }
        }
        this.pdFilters = pushed.toArray(new Filter[0]);
        return unhandled.toArray(new Filter[0]);
    }

    /**
     * For SupportsPushDownFilters interface
     * @return - the pushed-down filters
     */
    public Filter[] pushedFilters() {
        return this.pdFilters;
    }

    /**
     * Accepts Spark V2 predicates that the Flight SQL server can evaluate exactly.
     *
     * @param predicates candidate predicates
     * @return predicates that still need evaluation in Spark
     */
    @Override
    public Predicate[] pushPredicates(Predicate[] predicates) {
        LOGGER.debug("{}.pushPredicates()", this.getClass().getName());

        List<Predicate> pushed = new ArrayList<>();
        List<Predicate> unhandled = new ArrayList<>();
        for (Predicate predicate : predicates) {
            if (this.table.canPushPredicate(predicate)) {
                pushed.add(predicate);
            } else {
                unhandled.add(predicate);
            }
        }
        this.pdPredicates = pushed.toArray(new Predicate[0]);
        this.retained = !pushed.isEmpty() && retainsFilters(pushed);
        if (this.retained) {
            unhandled.addAll(pushed);
        }
        return unhandled.toArray(new Predicate[0]);
    }

    /**
     * Chooses whether accepted predicates stay in Spark's plan as well.
     *
     * @param retain true to return accepted predicates to Spark
     */
    void retainPushedFilters(boolean retain) {
        this.retainMode = Boolean.toString(retain);
    }

    private boolean retainsFilters(List<Predicate> pushed) {
        if (!"auto".equalsIgnoreCase(this.retainMode)) {
            return shouldRetain(this.retainMode, -1, 1.0, this.retainMaxRows, this.retainMaxSelectivity);
        }
        StructType schema = this.table.getSparkSchema();
        if (this.table.getName() == null || schema == null || schema.fields().length == 0) {
            return false;
        }
        Predicate[] predicates = pushed.toArray(new Predicate[0]);
        Set<String> columns = new LinkedHashSet<>(FilterSelectivity.columns(this.pdFilters, predicates));
        columns.add(schema.fields()[0].name());
        return FlightColumnStatistics.fetch(this.configuration, this.table.getName(), columns).map(answer -> {
            Map<String, FlightColumnStatistics.Column> byName = answer.columns() == null ? Map.of()
                    : answer.columns();
            double selectivity = FilterSelectivity.of(this.pdFilters, predicates, byName, answer.rows());
            return shouldRetain(this.retainMode, answer.rows(), selectivity, this.retainMaxRows,
                    this.retainMaxSelectivity);
        }).orElse(false);
    }

    /**
     * Decides whether pushed filters stay in Spark's plan. Runtime filtering collects the join keys of the filtered
     * side on the driver, so it only pays for a small table cut down to a small share of its rows.
     *
     * @param mode {@code true}, {@code false} or {@code auto}
     * @param rows rows in the table, or -1 when unknown
     * @param selectivity estimated share of the rows the filters keep
     * @param maxRows largest table kept in {@code auto} mode
     * @param maxSelectivity largest kept share in {@code auto} mode
     * @return true to return the pushed predicates to Spark as well
     */
    static boolean shouldRetain(String mode, long rows, double selectivity, long maxRows, double maxSelectivity) {
        if ("true".equalsIgnoreCase(mode)) {
            return true;
        }
        return "auto".equalsIgnoreCase(mode) && rows >= 0 && rows <= maxRows && selectivity <= maxSelectivity;
    }

    static String sparkSetting(String key, String fallback) {
        try {
            return org.apache.spark.sql.SparkSession.active().conf().get(key, fallback);
        } catch (RuntimeException e) {
            return fallback;
        }
    }

    /**
     * Returns the Spark V2 predicates accepted by this scan.
     *
     * @return pushed V2 predicates
     */
    @Override
    public Predicate[] pushedPredicates() {
        return this.pdPredicates;
    }

    /**
     * For SupportsPushDownRequiredColumns interface
     * @param columns - the schema containing the required columns
     */
    @Override
    public void pruneColumns(StructType columns) {
        this.pdColumns = columns.fields();
        LOGGER.debug("{}.pruneColumns(): columns={}", this.getClass().getName(),
                Arrays.toString(Arrays.stream(this.pdColumns)
                        .map(StructField::name).toArray(String[]::new)));
    }

    /**
     * To build a flight-scan
     * @return - A flight scan
     */
    @Override
    public Scan build() {
        LOGGER.debug("{}.build()", this.getClass().getName());

        //adjust flight-table upon pushed filters & columns
        List<String> whereParts = new ArrayList<>();
        Arrays.stream(this.pdFilters).map(this.table::toWhereClause)
                .forEach(whereParts::add);
        Arrays.stream(this.pdPredicates).map(this.table::toWhereClause)
                .forEach(whereParts::add);
        String where = String.join(" and ", whereParts);
        LOGGER.debug("{}.build(): columns={} filters={} predicates={}",
                this.getClass().getName(),
                Arrays.toString(Arrays.stream(this.pdColumns)
                        .map(StructField::name).toArray(String[]::new)),
                this.pdFilters.length, this.pdPredicates.length);
        this.table.probe(where, this.pdColumns, this.pdAggregation, this.partitionBehavior);
        // Plan exactly once, after Spark has supplied every accepted pushdown.
        // Earlier schema-only planning creates unused server handles and makes
        // Flight latency look worse than the actual scan.
        this.table.initialize(this.configuration);
        List<String> baseParts = List.copyOf(whereParts);
        StructField[] columns = this.pdColumns;
        PushAggregation aggregation = this.pdAggregation;
        FlightScan.Replanner replanner = extra -> {
            List<String> all = new ArrayList<>(baseParts);
            all.addAll(extra);
            this.table.probe(String.join(" and ", all), columns, aggregation, this.partitionBehavior);
            this.table.initialize(this.configuration);
        };
        return new FlightScan(this.configuration, this.table, aggregation != null, replanner, this.pdFilters,
                this.pdPredicates, this.retained);
    }
}
