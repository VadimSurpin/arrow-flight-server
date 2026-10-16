package pro.surpin.data.arrowflight.client.spark.read;

import pro.surpin.data.arrowflight.client.Configuration;
import pro.surpin.data.arrowflight.client.write.PartitionBehavior;
import pro.surpin.data.arrowflight.client.query.PushAggregation;
import pro.surpin.data.arrowflight.client.model.Table;
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
import java.util.List;

/**
 * Build flight scans which supports pushed-down filter, fields & aggregates
 */
public final class FlightScanBuilder implements ScanBuilder, SupportsPushDownV2Filters,
        SupportsPushDownRequiredColumns, SupportsPushDownAggregates {
    private static final Logger LOGGER = LoggerFactory.getLogger(FlightScanBuilder.class);

    private final Configuration configuration;
    private final Table table;
    private final PartitionBehavior partitionBehavior;

    //the pushed-down filters
    private Filter[] pdFilters = new Filter[0];
    //the pushed-down V2 predicates
    private Predicate[] pdPredicates = new Predicate[0];
    //the pushed-down columns
    private StructField[] pdColumns = new StructField[0];

    //pushed-down aggregation
    private PushAggregation pdAggregation = null;

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
        return unhandled.toArray(new Predicate[0]);
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
        return new FlightScan(this.configuration, this.table);
    }
}
