package pro.surpin.data.arrowflight.client.spark.read;

import pro.surpin.data.arrowflight.client.Configuration;
import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;
import org.apache.spark.sql.vectorized.ColumnarBatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Creates row or columnar readers for Flight input partitions.
 */
public class FlightPartitionReaderFactory implements PartitionReaderFactory {
    private static final Logger LOGGER = LoggerFactory.getLogger(FlightPartitionReaderFactory.class);

    private final Configuration configuration;

    /**
     * Construct a flight partition-reader factory
     * @param configuration - the configuration of remote flight service
     */
    public FlightPartitionReaderFactory(Configuration configuration) {
        this.configuration = configuration;
    }

    /**
     * Create a reader
     * @param inputPartition - the input-partition for the reader
     * @return - a partition-reader
     */
    @Override
    public PartitionReader<InternalRow> createReader(InputPartition inputPartition) {
        LOGGER.debug("{}.createReader()", this.getClass().getName());
        return new FlightPartitionReader(this.configuration, inputPartition);
    }

    @Override
    public boolean supportColumnarReads(InputPartition inputPartition) {
        if (!(inputPartition instanceof FlightInputPartition flightPartition)) {
            return false;
        }
        try {
            Schema schema = flightPartition.getSchema();
            return schema.getFields().stream()
                    .allMatch(FlightPartitionReaderFactory::isColumnarType);
        } catch (IOException e) {
            LOGGER.warn("Cannot inspect Flight partition schema for columnar read: {}",
                    e.getMessage());
            return false;
        }
    }

    @Override
    public PartitionReader<ColumnarBatch> createColumnarReader(InputPartition inputPartition) {
        LOGGER.debug("{}.createColumnarReader()", this.getClass().getName());
        return new FlightColumnarPartitionReader(this.configuration, inputPartition);
    }

    /**
     * Checks whether a field has an exact Spark columnar adapter.
     * @param field Arrow field
     * @return true when the field can use the columnar reader
     */
    private static boolean isColumnarType(Field field) {
        if (field.getDictionary() != null) {
            return false;
        }
        ArrowType type = field.getType();
        return switch (type.getTypeID()) {
            case Bool, Utf8, LargeUtf8, Binary, LargeBinary, Null, FixedSizeBinary, Timestamp -> true;
            case FloatingPoint -> {
                FloatingPointPrecision precision =
                        ((ArrowType.FloatingPoint) type).getPrecision();
                yield precision == FloatingPointPrecision.SINGLE
                        || precision == FloatingPointPrecision.DOUBLE;
            }
            case Int -> {
                ArrowType.Int integer = (ArrowType.Int) type;
                int bitWidth = integer.getBitWidth();
                yield bitWidth == 8 || bitWidth == 16
                        || bitWidth == 32 || bitWidth == 64;
            }
            case Decimal -> {
                ArrowType.Decimal decimal = (ArrowType.Decimal) type;
                int precision = decimal.getPrecision();
                int scale = decimal.getScale();
                yield decimal.getBitWidth() == 128
                        && precision >= 1 && precision <= 38
                        && scale >= 0 && scale <= precision;
            }
            case Date -> {
                DateUnit unit = ((ArrowType.Date) type).getUnit();
                yield unit == DateUnit.DAY || unit == DateUnit.MILLISECOND;
            }
            default -> false;
        };
    }
}
