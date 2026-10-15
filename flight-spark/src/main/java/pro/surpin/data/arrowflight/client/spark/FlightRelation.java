package pro.surpin.data.arrowflight.client.spark;

import pro.surpin.data.arrowflight.client.Configuration;
import pro.surpin.data.arrowflight.client.model.Table;
import pro.surpin.data.arrowflight.client.spark.read.FlightBatch;
import pro.surpin.data.arrowflight.client.spark.read.FlightPartitionReader;
import org.apache.spark.TaskContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.sources.BaseRelation;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.sources.PrunedFilteredScan;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * Spark DataSource V1 bridge used by Spark Thrift Server for persisted Hive tables.
 */
public final class FlightRelation extends BaseRelation implements PrunedFilteredScan {
    private final SQLContext sqlContext;
    private final Configuration configuration;
    private final Table table;
    private final StructType schema;

    public FlightRelation(SQLContext sqlContext, Configuration configuration, Table table) {
        this.sqlContext = sqlContext;
        this.configuration = configuration;
        this.table = table;
        this.schema = table.getSparkSchema();
    }

    @Override
    public SQLContext sqlContext() {
        return this.sqlContext;
    }

    @Override
    public StructType schema() {
        return this.schema;
    }

    /**
     * The scan already produces Catalyst {@link InternalRow}s. Telling Spark not to
     * convert them avoids materializing a second Object[] and external Row per record.
     */
    @Override
    public boolean needConversion() {
        return false;
    }

    @Override
    public Filter[] unhandledFilters(Filter[] filters) {
        if (filters == null || filters.length == 0) {
            return new Filter[0];
        }
        return Arrays.stream(filters)
                .filter(filter -> !this.table.canPushFilter(filter))
                .toArray(Filter[]::new);
    }

    @Override
    public RDD<Row> buildScan(String[] requiredColumns, Filter[] filters) {
        StructField[] projectedFields = resolveRequiredFields(requiredColumns);
        StructField[] queryFields = projectedFields;
        if (queryFields.length == 0 && this.schema.fields().length > 0) {
            // A zero-column Spark scan (for example COUNT(*)) still needs one remote
            // value per source row to preserve cardinality.
            queryFields = new StructField[] { this.schema.fields()[0] };
        }

        Filter[] scanFilters = filters == null ? new Filter[0] : filters;
        String where = String.join(" and ", Arrays.stream(scanFilters)
                .filter(this.table::canPushFilter)
                .map(this.table::toWhereClause)
                .toArray(String[]::new));

        // Table contains mutable statement/schema/endpoint state. Give every Spark
        // scan an independent instance so concurrent or repeated plans cannot reuse
        // endpoints created for an older projection/filter.
        Table scanTable = this.table.newScan();
        scanTable.probe(where, queryFields, null, null);
        scanTable.initialize(this.configuration);

        InputPartition[] partitions = new FlightBatch(this.configuration, scanTable).planInputPartitions();
        JavaSparkContext sparkContext = JavaSparkContext.fromSparkContext(this.sqlContext.sparkContext());
        if (partitions.length == 0) {
            return asRowRdd(sparkContext.<InternalRow>emptyRDD());
        }

        Configuration scanConfiguration = this.configuration;
        JavaRDD<InputPartition> partitionRdd = sparkContext.parallelize(Arrays.asList(partitions), partitions.length);
        JavaRDD<InternalRow> rows = partitionRdd.mapPartitions(partitionIterator -> {
            FlightRowIterator iterator = new FlightRowIterator(scanConfiguration, partitionIterator);
            TaskContext taskContext = TaskContext.get();
            if (taskContext != null) {
                taskContext.addTaskCompletionListener(
                        (org.apache.spark.util.TaskCompletionListener) context -> iterator.close());
            }
            return iterator;
        });
        return asRowRdd(rows);
    }

    private StructField[] resolveRequiredFields(String[] requiredColumns) {
        if (requiredColumns == null) {
            return this.schema.fields();
        }
        StructField[] fields = new StructField[requiredColumns.length];
        StructField[] relationFields = this.schema.fields();
        for (int i = 0; i < requiredColumns.length; i++) {
            String required = requiredColumns[i];
            StructField match = Arrays.stream(relationFields)
                    .filter(field -> field.name().equals(required))
                    .findFirst()
                    .orElseGet(() -> Arrays.stream(relationFields)
                            .filter(field -> field.name().equalsIgnoreCase(required))
                            .findFirst()
                            .orElseThrow(() -> new IllegalArgumentException(
                                    "Required column is absent from Flight schema: " + required)));
            fields[i] = match;
        }
        return fields;
    }

    /**
     * Spark's V1 interface is typed as RDD&lt;Row&gt;, while needConversion=false means
     * the physical scan consumes the contained InternalRows directly (the same
     * convention used by Spark's JDBC V1 relation).
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    private static RDD<Row> asRowRdd(JavaRDD<InternalRow> rows) {
        return (RDD) rows.rdd();
    }

    private static final class FlightRowIterator implements Iterator<InternalRow>, AutoCloseable {
        private final Configuration configuration;
        private final Iterator<InputPartition> partitions;

        private FlightPartitionReader reader;
        private InternalRow nextRow;
        private boolean loaded;

        private FlightRowIterator(Configuration configuration, Iterator<InputPartition> partitions) {
            this.configuration = configuration;
            this.partitions = partitions;
        }

        @Override
        public boolean hasNext() {
            if (!this.loaded) {
                this.advance();
            }
            return this.nextRow != null;
        }

        @Override
        public InternalRow next() {
            if (!this.hasNext()) {
                throw new NoSuchElementException();
            }
            InternalRow current = this.nextRow;
            this.nextRow = null;
            this.loaded = false;
            return current;
        }

        private void advance() {
            this.loaded = true;
            this.nextRow = null;

            while (true) {
                try {
                    if (this.reader != null && this.reader.next()) {
                        this.nextRow = this.reader.get();
                        return;
                    }
                    this.closeReader();
                    if (!this.partitions.hasNext()) {
                        return;
                    }
                    this.reader = new FlightPartitionReader(this.configuration, this.partitions.next());
                } catch (IOException e) {
                    this.closeReader();
                    throw new RuntimeException("Failed to read Flight partition", e);
                }
            }
        }

        @Override
        public void close() {
            this.closeReader();
        }

        private void closeReader() {
            if (this.reader != null) {
                try {
                    this.reader.close();
                } finally {
                    this.reader = null;
                }
            }
        }
    }
}
