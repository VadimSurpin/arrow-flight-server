package pro.surpin.data.arrowflight.client.model;

import org.apache.arrow.vector.*;
import org.apache.arrow.vector.complex.*;
import org.apache.arrow.vector.holders.*;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.UnsafeArrayData;
import org.apache.spark.sql.catalyst.expressions.UnsafeMapData;
import org.apache.spark.sql.catalyst.expressions.UnsafeRow;
import org.apache.spark.sql.catalyst.util.DateTimeUtils;
import org.apache.spark.sql.catalyst.util.IntervalUtils;
import org.apache.spark.sql.types.*;
import java.io.Serializable;
import java.math.BigDecimal;
import java.sql.Timestamp;
import java.time.*;
import java.util.Arrays;
import java.util.Map;
import java.util.stream.IntStream;

/**
 * Defines utility methods for transformation & conversion
 */
public final class ArrowConversion implements Serializable {
    private static final String FORMAT = "%s-%s";
    private static final String CONVERTER_NOT_FOUND_MSG = "The %s doesn't have a converter defined.";

    //the fromConversion interface
    @FunctionalInterface
    public interface ConvertFrom<X, Y, Z, R> {
        R apply(X x, Y y, Z z);
    }
    //the toConversion interface
    @FunctionalInterface
    public interface ConvertTo<A, B, C, D> {
        void apply(A a, B b, C c, D d);
    }

    //the from-converter container
    private final transient Map<String, ConvertFrom<org.apache.arrow.vector.FieldVector, Integer, FieldType, FieldVector>> fromConverters = new java.util.HashMap<>();
    //the to-converter container
    private final transient Map<String, ConvertTo<org.apache.arrow.vector.FieldVector, InternalRow[], Integer, DataType>> toConverters = new java.util.HashMap<>();
    //the to-object-converter container
    private final transient Map<String, ConvertTo<org.apache.arrow.vector.FieldVector, Integer, Object, DataType>> toObjectConverters = new java.util.HashMap<>();

    /**
     * Construct a Vector object
     */
    private ArrowConversion() {
        this.initializeFrom();
        this.initializeTo();
    }

    /**
     * Convert an arrow FieldVector into a custom FieldVector
     * @param vector - the arrow FieldVector
     * @param type - the type of all elements in the vector
     * @param rowCount - the number of rows in the vector
     * @return - a custom FieldVector hodling java-type objects
     */
    public FieldVector convert(org.apache.arrow.vector.FieldVector vector, FieldType type, int rowCount) {
        String key = String.format(FORMAT, vector.getClass().getTypeName(), type.getTypeID());
        if (!this.fromConverters.containsKey(key)) {
            throw new IllegalArgumentException(String.format(CONVERTER_NOT_FOUND_MSG, key));
        }
        return this.fromConverters.get(key).apply(vector, rowCount, type);
    }

    /**
     * Populate an arrow FieldVector with data in rows
     * @param vector - the target arrow vector
     * @param rows - the rows containing source data
     * @param idxColumn - the index of column in rows whose data is to be used for the population
     * @param type - the data-type in the target column.
     */
    public void populate(org.apache.arrow.vector.FieldVector vector, InternalRow[] rows, int idxColumn, DataType type) {
        String key = vector.getClass().getTypeName();
        if (!this.toConverters.containsKey(key)) {
            throw new IllegalArgumentException(String.format(CONVERTER_NOT_FOUND_MSG, key));
        }
        this.toConverters.get(key).apply(vector, rows, idxColumn, type);
    }
    /**
     * Populates a single value into an arrow vector at the given index.
     * @param vector target vector
     * @param index row index
     * @param value value to set
     * @param type Spark data type of the value
     */
    private void populateObject(org.apache.arrow.vector.FieldVector vector, int index, Object value, DataType type) {
        String key = vector.getClass().getTypeName();
        if (!this.toObjectConverters.containsKey(key)) {
            throw new IllegalArgumentException(String.format(CONVERTER_NOT_FOUND_MSG, key));
        }
        this.toObjectConverters.get(key).apply(vector, index, value, type);
    }

    /**
     * Initialize from-converters
     */
    private void initializeFrom() {
        this.fromConverters.put(String.format(FORMAT, TinyIntVector.class.getTypeName(), FieldType.IDs.BYTE), ArrowConversion.fromTinyInt);
        this.fromConverters.put(String.format(FORMAT, SmallIntVector.class.getTypeName(), FieldType.IDs.SHORT), ArrowConversion.fromSmallInt);
        this.fromConverters.put(String.format(FORMAT, IntVector.class.getTypeName(), FieldType.IDs.INT), ArrowConversion.fromInt);
        this.fromConverters.put(String.format(FORMAT, BigIntVector.class.getTypeName(), FieldType.IDs.LONG), ArrowConversion.fromBigInt);
        this.fromConverters.put(String.format(FORMAT, UInt1Vector.class.getTypeName(), FieldType.IDs.SHORT), ArrowConversion.fromUInt1);
        this.fromConverters.put(String.format(FORMAT, UInt2Vector.class.getTypeName(), FieldType.IDs.INT), ArrowConversion.fromUInt2);
        this.fromConverters.put(String.format(FORMAT, UInt4Vector.class.getTypeName(), FieldType.IDs.INT), ArrowConversion.fromUInt4Int);
        this.fromConverters.put(String.format(FORMAT, UInt4Vector.class.getTypeName(), FieldType.IDs.LONG), ArrowConversion.fromUInt4Long);
        this.fromConverters.put(String.format(FORMAT, UInt8Vector.class.getTypeName(), FieldType.IDs.LONG), ArrowConversion.fromUInt8Long);
        this.fromConverters.put(String.format(FORMAT, UInt8Vector.class.getTypeName(), FieldType.IDs.BIGINT), ArrowConversion.fromUInt8Bigint);
        this.fromConverters.put(String.format(FORMAT, Float4Vector.class.getTypeName(), FieldType.IDs.FLOAT), ArrowConversion.fromFloat4);
        this.fromConverters.put(String.format(FORMAT, Float8Vector.class.getTypeName(), FieldType.IDs.DOUBLE), ArrowConversion.fromFloat8);
        this.fromConverters.put(String.format(FORMAT, DecimalVector.class.getTypeName(), FieldType.IDs.DECIMAL), ArrowConversion.fromDecimal);
        this.fromConverters.put(String.format(FORMAT, Decimal256Vector.class.getTypeName(), FieldType.IDs.DECIMAL), ArrowConversion.fromDecimal256);
        this.fromConverters.put(String.format(FORMAT, VarCharVector.class.getTypeName(), FieldType.IDs.VARCHAR), ArrowConversion.fromVarChar);
        this.fromConverters.put(String.format(FORMAT, LargeVarCharVector.class.getTypeName(), FieldType.IDs.VARCHAR), ArrowConversion.fromLargeVarChar);
        this.fromConverters.put(String.format(FORMAT, BitVector.class.getTypeName(), FieldType.IDs.BOOLEAN), ArrowConversion.fromBit);
        this.fromConverters.put(String.format(FORMAT, DateDayVector.class.getTypeName(), FieldType.IDs.DATE), ArrowConversion.fromDateDay);
        this.fromConverters.put(String.format(FORMAT, DateMilliVector.class.getTypeName(), FieldType.IDs.DATE), ArrowConversion.fromDateMilli);
        this.fromConverters.put(String.format(FORMAT, TimeSecVector.class.getTypeName(), FieldType.IDs.TIME), ArrowConversion.fromTimeSec);
        this.fromConverters.put(String.format(FORMAT, TimeMilliVector.class.getTypeName(), FieldType.IDs.TIME), ArrowConversion.fromTimeMilli);
        this.fromConverters.put(String.format(FORMAT, TimeMicroVector.class.getTypeName(), FieldType.IDs.TIME), ArrowConversion.fromTimeMicro);
        this.fromConverters.put(String.format(FORMAT, TimeNanoVector.class.getTypeName(), FieldType.IDs.TIME), ArrowConversion.fromTimeNano);
        this.fromConverters.put(String.format(FORMAT, TimeStampMicroVector.class.getTypeName(), FieldType.IDs.TIMESTAMP), ArrowConversion.fromTimeStampMicro);
        this.fromConverters.put(String.format(FORMAT, TimeStampMicroTZVector.class.getTypeName(), FieldType.IDs.TIMESTAMP), ArrowConversion.fromTimeStampMicroTZ);
        this.fromConverters.put(String.format(FORMAT, TimeStampMilliVector.class.getTypeName(), FieldType.IDs.TIMESTAMP), ArrowConversion.fromTimeStampMilli);
        this.fromConverters.put(String.format(FORMAT, TimeStampMilliTZVector.class.getTypeName(), FieldType.IDs.TIMESTAMP), ArrowConversion.fromTimeStampMilliTZ);
        this.fromConverters.put(String.format(FORMAT, TimeStampSecVector.class.getTypeName(), FieldType.IDs.TIMESTAMP), ArrowConversion.fromTimeStampSec);
        this.fromConverters.put(String.format(FORMAT, TimeStampSecTZVector.class.getTypeName(), FieldType.IDs.TIMESTAMP), ArrowConversion.fromTimeStampSecTZ);
        this.fromConverters.put(String.format(FORMAT, TimeStampNanoVector.class.getTypeName(), FieldType.IDs.TIMESTAMP), ArrowConversion.fromTimeStampNano);

        this.fromConverters.put(String.format(FORMAT, TimeStampNanoVector.class.getTypeName(), FieldType.IDs.INT), ArrowConversion.fromTimeStampNanoToInt);

        this.fromConverters.put(String.format(FORMAT, TimeStampNanoTZVector.class.getTypeName(), FieldType.IDs.TIMESTAMP), ArrowConversion.fromTimeStampNanoTZ);
        this.fromConverters.put(String.format(FORMAT, IntervalYearVector.class.getTypeName(), FieldType.IDs.PERIOD_YEAR_MONTH), ArrowConversion.fromIntervalYear);
        this.fromConverters.put(String.format(FORMAT, IntervalDayVector.class.getTypeName(), FieldType.IDs.DURATION_DAY_TIME), ArrowConversion.fromIntervalDay);
        this.fromConverters.put(String.format(FORMAT, DurationVector.class.getTypeName(), FieldType.IDs.DURATION_DAY_TIME), ArrowConversion.fromDuration);
        this.fromConverters.put(String.format(FORMAT, IntervalMonthDayNanoVector.class.getTypeName(), FieldType.IDs.PERIOD_DURATION_MONTH_DAY_TIME), ArrowConversion.fromMonthDay);
        this.fromConverters.put(String.format(FORMAT, NullVector.class.getTypeName(), FieldType.IDs.NULL), ArrowConversion.fromNull);
        this.fromConverters.put(String.format(FORMAT, VarBinaryVector.class.getTypeName(), FieldType.IDs.BYTES), ArrowConversion.fromVarBinary);
        this.fromConverters.put(String.format(FORMAT, LargeVarBinaryVector.class.getTypeName(), FieldType.IDs.BYTES), ArrowConversion.fromLargeVarBinary);
        this.fromConverters.put(String.format(FORMAT, FixedSizeBinaryVector.class.getTypeName(), FieldType.IDs.BYTES), ArrowConversion.fromFixedSizeBinary);
        this.fromConverters.put(String.format(FORMAT, LargeListVector.class.getTypeName(), FieldType.IDs.LIST), ArrowConversion.fromLargeList);
        this.fromConverters.put(String.format(FORMAT, FixedSizeListVector.class.getTypeName(), FieldType.IDs.LIST), ArrowConversion.fromFixedSizeList);
        this.fromConverters.put(String.format(FORMAT, MapVector.class.getTypeName(), FieldType.IDs.MAP), ArrowConversion.fromMap);
        this.fromConverters.put(String.format(FORMAT, ListVector.class.getTypeName(), FieldType.IDs.LIST), ArrowConversion.fromList);
        this.fromConverters.put(String.format(FORMAT, StructVector.class.getTypeName(), FieldType.IDs.STRUCT), ArrowConversion.fromStruct);
    }
    //convert arrow TinyIntVector to FieldVector for BYTE
    private static final ConvertFrom<org.apache.arrow.vector.FieldVector, Integer, FieldType, FieldVector> fromTinyInt = (vector, size, type) -> new FieldVector(vector.getName(), type, IntStream.range(0, size).mapToObj((TypeConversionHelper.<TinyIntVector>cast(vector))::getObject).toArray(Object[]::new));
    //convert arrow SmallIntVector to FieldVector for SHORT
    private static final ConvertFrom<org.apache.arrow.vector.FieldVector, Integer, FieldType, FieldVector> fromSmallInt = (vector, size, type) -> new FieldVector(vector.getName(), type, IntStream.range(0, size).mapToObj((TypeConversionHelper.<SmallIntVector>cast(vector))::getObject).toArray(Object[]::new));
    //convert arrow IntVector to FieldVector for INT
    private static final ConvertFrom<org.apache.arrow.vector.FieldVector, Integer, FieldType, FieldVector> fromInt = (vector, size, type) -> new FieldVector(vector.getName(), type, IntStream.range(0, size).mapToObj((TypeConversionHelper.<IntVector>cast(vector))::getObject).toArray(Object[]::new));
    //convert arrow BigIntVector to FieldVector for LONG
    private static final ConvertFrom<org.apache.arrow.vector.FieldVector, Integer, FieldType, FieldVector> fromBigInt = (vector, size, type) -> new FieldVector(vector.getName(), type, IntStream.range(0, size).mapToObj((TypeConversionHelper.<BigIntVector>cast(vector))::getObject).toArray(Object[]::new));
    //convert arrow UInt1Vector to FieldVector for SHORT
    private static final ConvertFrom<org.apache.arrow.vector.FieldVector, Integer, FieldType, FieldVector> fromUInt1 = (vector, size, type) -> new FieldVector(vector.getName(), type, IntStream.range(0, size).mapToObj((TypeConversionHelper.<UInt1Vector>cast(vector))::getObject).toArray(Object[]::new));
    //convert arrow UInt2Vector to FieldVector for INT
    private static final ConvertFrom<org.apache.arrow.vector.FieldVector, Integer, FieldType, FieldVector> fromUInt2 = (vector, size, type) -> new FieldVector(vector.getName(), type, IntStream.range(0, size).mapToObj((TypeConversionHelper.<UInt2Vector>cast(vector))::getObject).toArray(Object[]::new));
    //convert arrow UInt4Vector to FieldVector for INT
    private static final ConvertFrom<org.apache.arrow.vector.FieldVector, Integer, FieldType, FieldVector> fromUInt4Int = (vector, size, type) -> new FieldVector(vector.getName(), type, IntStream.range(0, size).mapToObj((TypeConversionHelper.<UInt4Vector>cast(vector))::getObject).toArray(Object[]::new));
    //convert arrow UInt4Vector to FieldVector for LONG
    private static final ConvertFrom<org.apache.arrow.vector.FieldVector, Integer, FieldType, FieldVector> fromUInt4Long = (vector, size, type) -> new FieldVector(vector.getName(), type, IntStream.range(0, size).mapToObj((TypeConversionHelper.<UInt4Vector>cast(vector))::getObjectNoOverflow).toArray(Object[]::new));
    //convert arrow UInt8Vector to FieldVector for LONG
    private static final ConvertFrom<org.apache.arrow.vector.FieldVector, Integer, FieldType, FieldVector> fromUInt8Long = (vector, size, type) -> new FieldVector(vector.getName(), type, IntStream.range(0, size).mapToObj((TypeConversionHelper.<UInt8Vector>cast(vector))::getObject).toArray(Object[]::new));
    //convert arrow UInt8Vector to FieldVector for BIGINT
    private static final ConvertFrom<org.apache.arrow.vector.FieldVector, Integer, FieldType, FieldVector> fromUInt8Bigint = (vector, size, type) -> new FieldVector(vector.getName(), type, IntStream.range(0, size).mapToObj((TypeConversionHelper.<UInt8Vector>cast(vector))::getObjectNoOverflow).toArray(Object[]::new));
    //convert arrow Float4Vector to FieldVector for FLOAT
    private static final ConvertFrom<org.apache.arrow.vector.FieldVector, Integer, FieldType, FieldVector> fromFloat4 = (vector, size, type) -> new FieldVector(vector.getName(), type, IntStream.range(0, size).mapToObj((TypeConversionHelper.<Float4Vector>cast(vector))::getObject).toArray(Object[]::new));
    //convert arrow Float8Vector to FieldVector for DOUBLE
    private static final ConvertFrom<org.apache.arrow.vector.FieldVector, Integer, FieldType, FieldVector> fromFloat8 = (vector, size, type) -> new FieldVector(vector.getName(), type, IntStream.range(0, size).mapToObj((TypeConversionHelper.<Float8Vector>cast(vector))::getObject).toArray(Object[]::new));
    //convert arrow DecimalVector to FieldVector for DECIMAL
    private static final ConvertFrom<org.apache.arrow.vector.FieldVector, Integer, FieldType, FieldVector> fromDecimal = (vector, size, type) -> new FieldVector(vector.getName(), type, IntStream.range(0, size).mapToObj((TypeConversionHelper.<DecimalVector>cast(vector))::getObject).map(bd -> (bd == null) ? null : TypeConversionHelper.bigDecimalToDecimal.apply(bd)).toArray(Object[]::new));
    //convert arrow Decimal256Vector to FieldVector for DECIMAL
    private static final ConvertFrom<org.apache.arrow.vector.FieldVector, Integer, FieldType, FieldVector> fromDecimal256 = (vector, size, type) -> new FieldVector(vector.getName(), type, IntStream.range(0, size).mapToObj((TypeConversionHelper.<Decimal256Vector>cast(vector))::getObject).map(bd -> (bd == null) ? null : TypeConversionHelper.bigDecimalToDecimal.apply(bd)).toArray(Object[]::new));
    //convert arrow VarCharVector to FieldVector for STRING
    private static final ConvertFrom<org.apache.arrow.vector.FieldVector, Integer, FieldType, FieldVector> fromVarChar = (vector, size, type) -> new FieldVector(vector.getName(), type, IntStream.range(0, size).mapToObj((TypeConversionHelper.<VarCharVector>cast(vector))::getObject).map(s -> (s == null) ? null : TypeConversionHelper.stringToUtf8String.apply(s.toString())).toArray(Object[]::new));
    //convert arrow LargeVarCharVector to FieldVector for STRING
    private static final ConvertFrom<org.apache.arrow.vector.FieldVector, Integer, FieldType, FieldVector> fromLargeVarChar = (vector, size, type) -> new FieldVector(vector.getName(), type, IntStream.range(0, size).mapToObj((TypeConversionHelper.<LargeVarCharVector>cast(vector))::getObject).map(s -> (s == null) ? null : TypeConversionHelper.stringToUtf8String.apply(s.toString())).toArray(Object[]::new));
    //convert arrow BitVector to FieldVector for STRING
    private static final ConvertFrom<org.apache.arrow.vector.FieldVector, Integer, FieldType, FieldVector> fromBit = (vector, size, type) -> new FieldVector(vector.getName(), type, IntStream.range(0, size).mapToObj((TypeConversionHelper.<BitVector>cast(vector))::getObject).toArray(Object[]::new));
    //convert arrow DateDayVector to FieldVector for DATE
    private static final ConvertFrom<org.apache.arrow.vector.FieldVector, Integer, FieldType, FieldVector> fromDateDay = (vector, size, type) -> new FieldVector(vector.getName(), type, IntStream.range(0, size).mapToObj((TypeConversionHelper.<DateDayVector>cast(vector))::getObject).map(dd -> (dd == null) ? null : TypeConversionHelper.dateDayToInt.applyAsInt(dd)).toArray(Object[]::new));
    //convert arrow DateMilliVector to FieldVector for DATE
    private static final ConvertFrom<org.apache.arrow.vector.FieldVector, Integer, FieldType, FieldVector> fromDateMilli = (vector, size, type) -> new FieldVector(vector.getName(), type, IntStream.range(0, size).mapToObj((TypeConversionHelper.<DateMilliVector>cast(vector))::getObject).map(ldt -> (ldt == null) ? null : TypeConversionHelper.localDateTimeToInt.apply(ldt)).toArray(Object[]::new));
    //convert arrow TimeSecVector to FieldVector for TIME
    private static final ConvertFrom<org.apache.arrow.vector.FieldVector, Integer, FieldType, FieldVector> fromTimeSec = (vector, size, type) -> new FieldVector(vector.getName(), type, IntStream.range(0, size).mapToObj((TypeConversionHelper.<TimeSecVector>cast(vector))::getObject).map(ts -> (ts == null) ? null : TypeConversionHelper.timeSecToString.apply(ts)).toArray(Object[]::new));
    //convert arrow TimeMilliVector to FieldVector for TIME
    private static final ConvertFrom<org.apache.arrow.vector.FieldVector, Integer, FieldType, FieldVector> fromTimeMilli = (vector, size, type) -> new FieldVector(vector.getName(), type, IntStream.range(0, size).mapToObj((TypeConversionHelper.<TimeMilliVector>cast(vector))::getObject).map(tm -> (tm == null) ? null : TypeConversionHelper.timeMilliToString.apply(tm)).toArray(Object[]::new));
    //convert arrow TimeMicroVector to FieldVector for TIME
    private static final ConvertFrom<org.apache.arrow.vector.FieldVector, Integer, FieldType, FieldVector> fromTimeMicro = (vector, size, type) -> new FieldVector(vector.getName(), type, IntStream.range(0, size).mapToObj((TypeConversionHelper.<TimeMicroVector>cast(vector))::getObject).map(tm -> (tm == null) ? null : TypeConversionHelper.timeMicroToString.apply(tm)).toArray(Object[]::new));
    //convert arrow TimeNanoVector to FieldVector for TIME
    private static final ConvertFrom<org.apache.arrow.vector.FieldVector, Integer, FieldType, FieldVector> fromTimeNano = (vector, size, type) -> new FieldVector(vector.getName(), type, IntStream.range(0, size).mapToObj((TypeConversionHelper.<TimeNanoVector>cast(vector))::getObject).map(tn -> (tn == null) ? null : TypeConversionHelper.timeNanoToString.apply(tn)).toArray(Object[]::new));
    //convert arrow TimeStampMicroVector to FieldVector for TIMESTAMP
    private static final ConvertFrom<org.apache.arrow.vector.FieldVector, Integer, FieldType, FieldVector> fromTimeStampMicro = (vector, size, type) -> new FieldVector(vector.getName(), type, IntStream.range(0, size).mapToObj((TypeConversionHelper.<TimeStampMicroVector>cast(vector))::getObject).map(tsm -> (tsm == null) ? null : TypeConversionHelper.localDateTimeToLong.apply(tsm)).toArray(Object[]::new));
    //convert arrow TimeStampMicroTZVector to FieldVector for TIMESTAMP
    private static final ConvertFrom<org.apache.arrow.vector.FieldVector, Integer, FieldType, FieldVector> fromTimeStampMicroTZ = (vector, size, type) -> {
        TimeStampMicroTZVector value = TypeConversionHelper.cast(vector);
        return new FieldVector(vector.getName(), type, IntStream.range(0, size).mapToObj(value::getObject).map(tsm -> (tsm == null) ? null : TypeConversionHelper.timestampMicroTZToLong.apply(tsm, value.getTimeZone())).toArray(Object[]::new));
    };
    //convert arrow TimeStampMilliVector to FieldVector for TIMESTAMP
    private static final ConvertFrom<org.apache.arrow.vector.FieldVector, Integer, FieldType, FieldVector> fromTimeStampMilli = (vector, size, type) -> new FieldVector(vector.getName(), type, IntStream.range(0, size).mapToObj((TypeConversionHelper.<TimeStampMilliVector>cast(vector))::getObject).map(tsm -> (tsm == null) ? null : TypeConversionHelper.timestampToLong.apply(Timestamp.valueOf(tsm))).toArray(Object[]::new));
    //convert arrow TimeStampMilliTZVector to FieldVector for TIMESTAMP
    private static final ConvertFrom<org.apache.arrow.vector.FieldVector, Integer, FieldType, FieldVector> fromTimeStampMilliTZ = (vector, size, type) -> {
        TimeStampMilliTZVector value = TypeConversionHelper.cast(vector);
        return new FieldVector(vector.getName(), type, IntStream.range(0, size).mapToObj(value::getObject).map(tsm -> (tsm == null) ? null : TypeConversionHelper.timestampMilliTZToLong.apply(tsm, value.getTimeZone())).toArray(Object[]::new));
    };
    //convert arrow TimeStampSecVector to FieldVector for TIMESTAMP
    private static final ConvertFrom<org.apache.arrow.vector.FieldVector, Integer, FieldType, FieldVector> fromTimeStampSec = (vector, size, type) -> new FieldVector(vector.getName(), type, IntStream.range(0, size).mapToObj((TypeConversionHelper.<TimeStampSecVector>cast(vector))::getObject).map(ldt -> (ldt == null) ? null : TypeConversionHelper.localDateTimeToLong.apply(ldt)).toArray(Object[]::new));
    //convert arrow TimeStampSecTZVector to FieldVector for TIMESTAMP
    private static final ConvertFrom<org.apache.arrow.vector.FieldVector, Integer, FieldType, FieldVector> fromTimeStampSecTZ = (vector, size, type) -> {
        TimeStampSecTZVector value = TypeConversionHelper.cast(vector);
        return new FieldVector(vector.getName(), type, IntStream.range(0, size).mapToObj(value::getObject).map(l -> (l == null) ? null : TypeConversionHelper.timestampSecTZToLong.apply(l, value.getTimeZone())).toArray(Object[]::new));
    };
    //convert arrow TimeStampNanoVector to FieldVector for TIMESTAMP
    private static final ConvertFrom<org.apache.arrow.vector.FieldVector, Integer, FieldType, FieldVector> fromTimeStampNano = (vector, size, type) -> new FieldVector(vector.getName(), type, IntStream.range(0, size).mapToObj((TypeConversionHelper.<TimeStampNanoVector>cast(vector))::getObject).map(ldt -> (ldt == null) ? null : TypeConversionHelper.localDateTimeToLong.apply(ldt)).toArray(Object[]::new));

    private static final ConvertFrom<org.apache.arrow.vector.FieldVector, Integer, FieldType, FieldVector> fromTimeStampNanoToInt = (vector, size, type) -> new FieldVector(vector.getName(), type, IntStream.range(0, size).mapToObj((TypeConversionHelper.<TimeStampNanoVector>cast(vector))::getObject).map(ldt -> (ldt == null) ? null : TypeConversionHelper.localDateTimeToInt.apply(ldt)).toArray(Object[]::new));


    //convert arrow TimeStampNanoTZVector to FieldVector for TIMESTAMP
    private static final ConvertFrom<org.apache.arrow.vector.FieldVector, Integer, FieldType, FieldVector> fromTimeStampNanoTZ = (vector, size, type) -> {
        TimeStampNanoTZVector value = TypeConversionHelper.cast(vector);
        return new FieldVector(vector.getName(), type, IntStream.range(0, size).mapToObj(value::getObject).map(l -> (l == null) ? null : TypeConversionHelper.timestampNanoTZToLong.apply(l, value.getTimeZone())).toArray(Object[]::new));
    };
    //convert arrow IntervalYearVector to FieldVector for PERIOD_YEAR_MONTH
    private static final ConvertFrom<org.apache.arrow.vector.FieldVector, Integer, FieldType, FieldVector> fromIntervalYear = (vector, size, type) -> new FieldVector(vector.getName(), type, IntStream.range(0, size).mapToObj((TypeConversionHelper.<IntervalYearVector>cast(vector))::getObject).map(p -> (p == null) ? null : TypeConversionHelper.periodToInt.apply(p)).toArray(Object[]::new));
    //convert arrow IntervalDayVector to FieldVector for DURATION_DAY_TIME
    private static final ConvertFrom<org.apache.arrow.vector.FieldVector, Integer, FieldType, FieldVector> fromIntervalDay = (vector, size, type) -> new FieldVector(vector.getName(), type, IntStream.range(0, size).mapToObj((TypeConversionHelper.<IntervalDayVector>cast(vector))::getObject).map(d -> (d == null) ? null : TypeConversionHelper.durationToLong.apply(d)).toArray(Object[]::new));
    //convert arrow DurationVector to FieldVector for DURATION_DAY_TIME
    private static final ConvertFrom<org.apache.arrow.vector.FieldVector, Integer, FieldType, FieldVector> fromDuration = (vector, size, type) -> new FieldVector(vector.getName(), type, IntStream.range(0, size).mapToObj((TypeConversionHelper.<DurationVector>cast(vector))::getObject).map(d -> (d == null) ? null : TypeConversionHelper.durationToLong.apply(d)).toArray(Object[]::new));
    //convert arrow IntervalMonthDayNanoVector to FieldVector for PERIOD_DURATION_MONTH_DAY_TIME
    private static final ConvertFrom<org.apache.arrow.vector.FieldVector, Integer, FieldType, FieldVector> fromMonthDay = (vector, size, type) -> new FieldVector(vector.getName(), type, IntStream.range(0, size).mapToObj((TypeConversionHelper.<IntervalMonthDayNanoVector>cast(vector))::getObject).map(pd -> (pd == null) ? null : TypeConversionHelper.translatePeriodDuration.apply(pd)).toArray(Object[]::new));
    //convert arrow NullVector to FieldVector for NULL
    private static final ConvertFrom<org.apache.arrow.vector.FieldVector, Integer, FieldType, FieldVector> fromNull = (vector, size, type) -> new FieldVector(vector.getName(), type, IntStream.range(0, size).mapToObj((TypeConversionHelper.<NullVector>cast(vector))::getObject).toArray(Object[]::new));
    //convert arrow VarBinaryVector to FieldVector for BYTES
    private static final ConvertFrom<org.apache.arrow.vector.FieldVector, Integer, FieldType, FieldVector> fromVarBinary = (vector, size, type) -> new FieldVector(vector.getName(), type, IntStream.range(0, size).mapToObj((TypeConversionHelper.<VarBinaryVector>cast(vector))::getObject).toArray(Object[]::new));
    //convert arrow LargeVarBinaryVector to FieldVector for BYTES
    private static final ConvertFrom<org.apache.arrow.vector.FieldVector, Integer, FieldType, FieldVector> fromLargeVarBinary = (vector, size, type) -> new FieldVector(vector.getName(), type, IntStream.range(0, size).mapToObj((TypeConversionHelper.<LargeVarBinaryVector>cast(vector))::getObject).toArray(Object[]::new));
    //convert arrow FixedSizeBinaryVector to FieldVector for BYTES
    private static final ConvertFrom<org.apache.arrow.vector.FieldVector, Integer, FieldType, FieldVector> fromFixedSizeBinary = (vector, size, type) -> new FieldVector(vector.getName(), type, IntStream.range(0, size).mapToObj((TypeConversionHelper.<FixedSizeBinaryVector>cast(vector))::getObject).toArray(Object[]::new));
    //convert arrow LargeListVector to FieldVector for LIST
    private static final ConvertFrom<org.apache.arrow.vector.FieldVector, Integer, FieldType, FieldVector> fromLargeList = (vector, size, type) -> new FieldVector(vector.getName(), type, IntStream.range(0, size).mapToObj((TypeConversionHelper.<LargeListVector>cast(vector))::getObject).map(e -> (e == null) ? null : TypeConversionHelper.translateList.apply(e, TypeConversionHelper.cast(vector), (FieldType.ListType) type)).toArray(Object[]::new));
    //convert arrow FixedSizeListVector to FieldVector for LIST
    private static final ConvertFrom<org.apache.arrow.vector.FieldVector, Integer, FieldType, FieldVector> fromFixedSizeList = (vector, size, type) -> new FieldVector(vector.getName(), type, IntStream.range(0, size).mapToObj((TypeConversionHelper.<FixedSizeListVector>cast(vector))::getObject).map(e -> (e == null) ? null : TypeConversionHelper.translateList.apply(e, TypeConversionHelper.cast(vector), (FieldType.ListType) type)).toArray(Object[]::new));
    //convert arrow MapVector to FieldVector for MAP
    private static final ConvertFrom<org.apache.arrow.vector.FieldVector, Integer, FieldType, FieldVector> fromMap = (vector, size, type) -> new FieldVector(vector.getName(), type, IntStream.range(0, size).mapToObj((TypeConversionHelper.<MapVector>cast(vector))::getObject).map(e -> (e == null) ? null : TypeConversionHelper.translateMap.apply(e, TypeConversionHelper.cast(vector), (FieldType.MapType) type)).toArray(Object[]::new));
    //convert arrow ListVector to FieldVector for LIST
    private static final ConvertFrom<org.apache.arrow.vector.FieldVector, Integer, FieldType, FieldVector> fromList = (vector, size, type) -> new FieldVector(vector.getName(), type, IntStream.range(0, size).mapToObj((TypeConversionHelper.<ListVector>cast(vector))::getObject).map(e -> (e == null) ? null : TypeConversionHelper.translateList.apply(e, TypeConversionHelper.cast(vector), (FieldType.ListType) type)).toArray(Object[]::new));
    //convert arrow StructVector to FieldVector for STRUCT
    private static final ConvertFrom<org.apache.arrow.vector.FieldVector, Integer, FieldType, FieldVector> fromStruct = (vector, size, type) -> new FieldVector(vector.getName(), type, IntStream.range(0, size).mapToObj((TypeConversionHelper.<StructVector>cast(vector))::getObject).map(e -> (e == null) ? null : TypeConversionHelper.mapElseStruct.apply(e, TypeConversionHelper.cast(vector), (FieldType.StructType) type)).toArray(Object[]::new));

    /**
     * Initialize to-converters
     */
    private void initializeTo() {
        this.toConverters.put(MapVector.class.getTypeName(), ArrowConversion.toMap);
        this.toObjectConverters.put(MapVector.class.getTypeName(), ArrowConversion.toMapObject);
        this.toConverters.put(ListVector.class.getTypeName(), ArrowConversion.toList);
        this.toObjectConverters.put(ListVector.class.getTypeName(), ArrowConversion.toListObject);
        this.toConverters.put(StructVector.class.getTypeName(), ArrowConversion.toStruct);
        this.toObjectConverters.put(StructVector.class.getTypeName(), ArrowConversion.toStructObject);
        this.toConverters.put(IntervalDayVector.class.getTypeName(), ArrowConversion.toIntervalDay);
        this.toObjectConverters.put(IntervalDayVector.class.getTypeName(), ArrowConversion.toIntervalDayObject);
        this.toConverters.put(IntervalYearVector.class.getTypeName(), ArrowConversion.toIntervalYear);
        this.toObjectConverters.put(IntervalYearVector.class.getTypeName(), ArrowConversion.toIntervalYearObject);
        this.toConverters.put(DurationVector.class.getTypeName(), ArrowConversion.toDuration);
        this.toObjectConverters.put(DurationVector.class.getTypeName(), ArrowConversion.toDurationObject);
        this.toConverters.put(TimeStampNanoTZVector.class.getTypeName(), ArrowConversion.toTimeStampNanoTZ);
        this.toObjectConverters.put(TimeStampNanoTZVector.class.getTypeName(), ArrowConversion.toTimeStampNanoTZObject);
        this.toConverters.put(TimeStampNanoVector.class.getTypeName(), ArrowConversion.toTimeStampNano);
        this.toObjectConverters.put(TimeStampNanoVector.class.getTypeName(), ArrowConversion.toTimeStampNanoObject);
        this.toConverters.put(TimeStampSecTZVector.class.getTypeName(), ArrowConversion.toTimeStampSecTZ);
        this.toObjectConverters.put(TimeStampSecTZVector.class.getTypeName(), ArrowConversion.toTimeStampSecTZObject);
        this.toConverters.put(TimeStampSecVector.class.getTypeName(), ArrowConversion.toTimeStampSec);
        this.toObjectConverters.put(TimeStampSecVector.class.getTypeName(), ArrowConversion.toTimeStampSecObject);
        this.toConverters.put(TimeStampMilliTZVector.class.getTypeName(), ArrowConversion.toTimeStampMilliTZ);
        this.toObjectConverters.put(TimeStampMilliTZVector.class.getTypeName(), ArrowConversion.toTimeStampMilliTZObject);
        this.toConverters.put(TimeStampMilliVector.class.getTypeName(), ArrowConversion.toTimeStampMilli);
        this.toObjectConverters.put(TimeStampMilliVector.class.getTypeName(), ArrowConversion.toTimeStampMilliObject);
        this.toConverters.put(TimeStampMicroTZVector.class.getTypeName(), ArrowConversion.toTimeStampMicroTZ);
        this.toObjectConverters.put(TimeStampMicroTZVector.class.getTypeName(), ArrowConversion.toTimeStampMicroTZObject);
        this.toConverters.put(TimeStampMicroVector.class.getTypeName(), ArrowConversion.toTimeStampMicro);
        this.toObjectConverters.put(TimeStampMicroVector.class.getTypeName(), ArrowConversion.toTimeStampMicroObject);
        this.toConverters.put(TimeNanoVector.class.getTypeName(), ArrowConversion.toTimeNano);
        this.toObjectConverters.put(TimeNanoVector.class.getTypeName(), ArrowConversion.toTimeNanoObject);
        this.toConverters.put(TimeMicroVector.class.getTypeName(), ArrowConversion.toTimeMicro);
        this.toObjectConverters.put(TimeMicroVector.class.getTypeName(), ArrowConversion.toTimeMicroObject);
        this.toConverters.put(TimeMilliVector.class.getTypeName(), ArrowConversion.toTimeMilli);
        this.toObjectConverters.put(TimeMilliVector.class.getTypeName(), ArrowConversion.toTimeMilliObject);
        this.toConverters.put(TimeSecVector.class.getTypeName(), ArrowConversion.toTimeSec);
        this.toObjectConverters.put(TimeSecVector.class.getTypeName(), ArrowConversion.toTimeSecObject);
        this.toConverters.put(DateMilliVector.class.getTypeName(), ArrowConversion.toDateMilli);
        this.toObjectConverters.put(DateMilliVector.class.getTypeName(), ArrowConversion.toDateMilliObject);
        this.toConverters.put(DateDayVector.class.getTypeName(), ArrowConversion.toDateDay);
        this.toObjectConverters.put(DateDayVector.class.getTypeName(), ArrowConversion.toDateDayObject);
        this.toConverters.put(BitVector.class.getTypeName(), ArrowConversion.toBit);
        this.toObjectConverters.put(BitVector.class.getTypeName(), ArrowConversion.toBitObject);
        this.toConverters.put(LargeVarCharVector.class.getTypeName(), ArrowConversion.toLargeVarChar);
        this.toObjectConverters.put(LargeVarCharVector.class.getTypeName(), ArrowConversion.toLargeVarCharObject);
        this.toConverters.put(VarCharVector.class.getTypeName(), ArrowConversion.toVarChar);
        this.toObjectConverters.put(VarCharVector.class.getTypeName(), ArrowConversion.toVarCharObject);
        this.toConverters.put(Decimal256Vector.class.getTypeName(), ArrowConversion.toDecimal256);
        this.toObjectConverters.put(Decimal256Vector.class.getTypeName(), ArrowConversion.toDecimal256Object);
        this.toConverters.put(DecimalVector.class.getTypeName(), ArrowConversion.toDecimal);
        this.toObjectConverters.put(DecimalVector.class.getTypeName(), ArrowConversion.toDecimalObject);
        this.toConverters.put(Float8Vector.class.getTypeName(), ArrowConversion.toFloat8);
        this.toObjectConverters.put(Float8Vector.class.getTypeName(), ArrowConversion.toFloat8Object);
        this.toConverters.put(Float4Vector.class.getTypeName(), ArrowConversion.toFloat4);
        this.toObjectConverters.put(Float4Vector.class.getTypeName(), ArrowConversion.toFloat4Object);
        this.toConverters.put(UInt8Vector.class.getTypeName(), ArrowConversion.toUInt8);
        this.toObjectConverters.put(UInt8Vector.class.getTypeName(), ArrowConversion.toUInt8Object);
        this.toConverters.put(UInt4Vector.class.getTypeName(), ArrowConversion.toUInt4);
        this.toObjectConverters.put(UInt4Vector.class.getTypeName(), ArrowConversion.toUInt4Object);
        this.toConverters.put(UInt2Vector.class.getTypeName(), ArrowConversion.toUInt2);
        this.toObjectConverters.put(UInt2Vector.class.getTypeName(), ArrowConversion.toUInt2Object);
        this.toConverters.put(UInt1Vector.class.getTypeName(), ArrowConversion.toUInt1);
        this.toObjectConverters.put(UInt1Vector.class.getTypeName(), ArrowConversion.toUInt1Object);
        this.toConverters.put(BigIntVector.class.getTypeName(), ArrowConversion.toBigInt);
        this.toObjectConverters.put(BigIntVector.class.getTypeName(), ArrowConversion.toBigIntObject);
        this.toConverters.put(IntVector.class.getTypeName(), ArrowConversion.toInt);
        this.toObjectConverters.put(IntVector.class.getTypeName(), ArrowConversion.toIntObject);
        this.toConverters.put(SmallIntVector.class.getTypeName(), ArrowConversion.toSmallInt);
        this.toObjectConverters.put(SmallIntVector.class.getTypeName(), ArrowConversion.toSmallIntObject);
        this.toConverters.put(TinyIntVector.class.getTypeName(), ArrowConversion.toTinyInt);
        this.toObjectConverters.put(TinyIntVector.class.getTypeName(), ArrowConversion.toTinyIntObject);
    }
    //convert BYTE to arrow TinyIntVector
    private static final ConvertTo<org.apache.arrow.vector.FieldVector, InternalRow[], Integer, DataType> toTinyInt = (vector, rows, idxColumn, type) -> IntStream.range(0, rows.length).forEach(idxRow -> ArrowConversion.toTinyIntObject.apply(vector, idxRow, rows[idxRow].get(idxColumn, type), type));
    private static final NullableTinyIntHolder nullTinyInt = new NullableTinyIntHolder();
    private static final ConvertTo<org.apache.arrow.vector.FieldVector, Integer, Object, DataType> toTinyIntObject = (vector, row, value, type) -> {
        if (value == null) {
            TypeConversionHelper.<TinyIntVector>cast(vector).setSafe(row, ArrowConversion.nullTinyInt);
        } else {
            TypeConversionHelper.<TinyIntVector>cast(vector).setSafe(row, (value instanceof Number) ? (Byte) value : Byte.parseByte(value.toString()));
        }
    };
    //convert SHORT to arrow SmallIntVector
    private static final ConvertTo<org.apache.arrow.vector.FieldVector, InternalRow[], Integer, DataType> toSmallInt = (vector, rows, idxColumn, type) -> IntStream.range(0, rows.length).forEach(idxRow -> ArrowConversion.toSmallIntObject.apply(vector, idxRow, rows[idxRow].get(idxColumn, type), type));
    private static final NullableSmallIntHolder nullSmallInt = new NullableSmallIntHolder();
    private static final ConvertTo<org.apache.arrow.vector.FieldVector, Integer, Object, DataType> toSmallIntObject = (vector, row, value, type) -> {
        if (value == null) {
            TypeConversionHelper.<SmallIntVector>cast(vector).setSafe(row, ArrowConversion.nullSmallInt);
        } else {
            TypeConversionHelper.<SmallIntVector>cast(vector).setSafe(row, (value instanceof Number) ? (Short) value : Short.parseShort(value.toString()));
        }
    };
    //convert INT to arrow IntVector
    private static final ConvertTo<org.apache.arrow.vector.FieldVector, InternalRow[], Integer, DataType> toInt = (vector, rows, idxColumn, type) -> IntStream.range(0, rows.length).forEach(idxRow -> ArrowConversion.toIntObject.apply(vector, idxRow, rows[idxRow].get(idxColumn, type), type));
    private static final NullableIntHolder nullInt = new NullableIntHolder();
    private static final ConvertTo<org.apache.arrow.vector.FieldVector, Integer, Object, DataType> toIntObject = (vector, row, value, type) -> {
        if (value == null) {
            TypeConversionHelper.<IntVector>cast(vector).setSafe(row, ArrowConversion.nullInt);
        } else {
            TypeConversionHelper.<IntVector>cast(vector).setSafe(row, (value instanceof Number) ? (Integer) value : Integer.parseInt(value.toString()));
        }
    };
    //convert LONG to arrow BigIntVector
    private static final ConvertTo<org.apache.arrow.vector.FieldVector, InternalRow[], Integer, DataType> toBigInt = (vector, rows, idxColumn, type) -> IntStream.range(0, rows.length).forEach(idxRow -> ArrowConversion.toBigIntObject.apply(vector, idxRow, rows[idxRow].get(idxColumn, type), type));
    private static final NullableBigIntHolder nullBigInt = new NullableBigIntHolder();
    private static final ConvertTo<org.apache.arrow.vector.FieldVector, Integer, Object, DataType> toBigIntObject = (vector, row, value, type) -> {
        if (value == null) {
            TypeConversionHelper.<BigIntVector>cast(vector).setSafe(row, ArrowConversion.nullBigInt);
        } else {
            TypeConversionHelper.<BigIntVector>cast(vector).setSafe(row, (value instanceof Number) ? (Long) value : Long.parseLong(value.toString()));
        }
    };
    //convert SHORT to arrow UInt1Vector
    private static final ConvertTo<org.apache.arrow.vector.FieldVector, InternalRow[], Integer, DataType> toUInt1 = (vector, rows, idxColumn, type) -> IntStream.range(0, rows.length).forEach(idxRow -> ArrowConversion.toUInt1Object.apply(vector, idxRow, rows[idxRow].get(idxColumn, type), type));
    private static final NullableUInt1Holder nullUInt1 = new NullableUInt1Holder();
    private static final ConvertTo<org.apache.arrow.vector.FieldVector, Integer, Object, DataType> toUInt1Object = (vector, row, value, type) -> {
        if (value == null) {
            TypeConversionHelper.<UInt1Vector>cast(vector).setSafe(row, ArrowConversion.nullUInt1);
        } else {
            TypeConversionHelper.<UInt1Vector>cast(vector).setSafe(row, (value instanceof Number) ? (Short) value : Short.parseShort(value.toString()));
        }
    };
    //convert INT to arrow UInt2Vector
    private static final ConvertTo<org.apache.arrow.vector.FieldVector, InternalRow[], Integer, DataType> toUInt2 = (vector, rows, idxColumn, type) -> IntStream.range(0, rows.length).forEach(idxRow -> ArrowConversion.toUInt2Object.apply(vector, idxRow, rows[idxRow].get(idxColumn, type), type));
    private static final NullableUInt2Holder nullUInt2 = new NullableUInt2Holder();
    private static final ConvertTo<org.apache.arrow.vector.FieldVector, Integer, Object, DataType> toUInt2Object = (vector, row, value, type) -> {
        if (value == null) {
            TypeConversionHelper.<UInt2Vector>cast(vector).setSafe(row, ArrowConversion.nullUInt2);
        } else {
            TypeConversionHelper.<UInt2Vector>cast(vector).setSafe(row, (value instanceof Number) ? (Integer) value : Integer.parseInt(value.toString()));
        }
    };
    //convert INT to arrow UInt4Vector
    private static final ConvertTo<org.apache.arrow.vector.FieldVector, InternalRow[], Integer, DataType> toUInt4 = (vector, rows, idxColumn, type) -> IntStream.range(0, rows.length).forEach(idxRow -> ArrowConversion.toUInt4Object.apply(vector, idxRow, rows[idxRow].get(idxColumn, type), type));
    private static final NullableUInt4Holder nullUInt4 = new NullableUInt4Holder();
    private static final ConvertTo<org.apache.arrow.vector.FieldVector, Integer, Object, DataType> toUInt4Object = (vector, row, value, type) -> {
        if (value == null) {
            TypeConversionHelper.<UInt4Vector>cast(vector).setSafe(row, ArrowConversion.nullUInt4);
        } else {
            TypeConversionHelper.<UInt4Vector>cast(vector).setSafe(row, (value instanceof Number) ? (Integer) value : Integer.parseInt(value.toString()));
        }
    };
    //convert LONG to arrow UInt8Vector
    private static final ConvertTo<org.apache.arrow.vector.FieldVector, InternalRow[], Integer, DataType> toUInt8 = (vector, rows, idxColumn, type) -> IntStream.range(0, rows.length).forEach(idxRow -> ArrowConversion.toUInt8Object.apply(vector, idxRow, rows[idxRow].get(idxColumn, type), type));
    private static final NullableUInt8Holder nullUInt8 = new NullableUInt8Holder();
    private static final ConvertTo<org.apache.arrow.vector.FieldVector, Integer, Object, DataType> toUInt8Object = (vector, row, value, type) -> {
        if (value == null) {
            TypeConversionHelper.<UInt8Vector>cast(vector).setSafe(row, ArrowConversion.nullUInt8);
        } else {
            TypeConversionHelper.<UInt8Vector>cast(vector).setSafe(row, (value instanceof Number) ? (Long) value : Long.parseLong(value.toString()));
        }
    };
    //convert FLOAT to arrow Float4Vector
    private static final ConvertTo<org.apache.arrow.vector.FieldVector, InternalRow[], Integer, DataType> toFloat4 = (vector, rows, idxColumn, type) -> IntStream.range(0, rows.length).forEach(idxRow -> ArrowConversion.toFloat4Object.apply(vector, idxRow, rows[idxRow].get(idxColumn, type), type));
    private static final NullableFloat4Holder nullFloat4 = new NullableFloat4Holder();
    private static final ConvertTo<org.apache.arrow.vector.FieldVector, Integer, Object, DataType> toFloat4Object = (vector, row, value, type) -> {
        if (value == null) {
            TypeConversionHelper.<Float4Vector>cast(vector).setSafe(row, ArrowConversion.nullFloat4);
        } else {
            TypeConversionHelper.<Float4Vector>cast(vector).setSafe(row, (value instanceof Number) ? (Float) value : Float.parseFloat(value.toString()));
        }
    };
    //convert DOUBLE to arrow Float8Vector
    private static final ConvertTo<org.apache.arrow.vector.FieldVector, InternalRow[], Integer, DataType> toFloat8 = (vector, rows, idxColumn, type) -> IntStream.range(0, rows.length).forEach(idxRow -> ArrowConversion.toFloat8Object.apply(vector, idxRow, rows[idxRow].get(idxColumn, type), type));
    private static final NullableFloat8Holder nullFloat8 = new NullableFloat8Holder();
    private static final ConvertTo<org.apache.arrow.vector.FieldVector, Integer, Object, DataType> toFloat8Object = (vector, row, value, type) -> {
        if (value == null) {
            TypeConversionHelper.<Float8Vector>cast(vector).setSafe(row, ArrowConversion.nullFloat8);
        } else {
            TypeConversionHelper.<Float8Vector>cast(vector).setSafe(row, (value instanceof Number) ? (Double) value : Double.parseDouble(value.toString()));
        }
    };
    //convert DECIMAL to arrow DecimalVector
    private static final ConvertTo<org.apache.arrow.vector.FieldVector, InternalRow[], Integer, DataType> toDecimal = (vector, rows, idxColumn, type) -> IntStream.range(0, rows.length).forEach(idxRow -> ArrowConversion.toDecimalObject.apply(vector, idxRow, rows[idxRow].get(idxColumn, type), type));
    private static final NullableDecimalHolder nullDecimal = new NullableDecimalHolder();
    private static final ConvertTo<org.apache.arrow.vector.FieldVector, Integer, Object, DataType> toDecimalObject = (vector, row, value, type) -> {
        if (value == null) {
            TypeConversionHelper.<DecimalVector>cast(vector).setSafe(row, ArrowConversion.nullDecimal);
        } else {
            TypeConversionHelper.<DecimalVector>cast(vector).setSafe(row, value instanceof Decimal decimal
                    ? decimal.toJavaBigDecimal() : BigDecimal.valueOf(Double.parseDouble(value.toString())));
        }
    };
    //convert DECIMAL to arrow Decimal256Vector
    private static final ConvertTo<org.apache.arrow.vector.FieldVector, InternalRow[], Integer, DataType> toDecimal256 = (vector, rows, idxColumn, type) -> IntStream.range(0, rows.length).forEach(idxRow -> ArrowConversion.toDecimal256Object.apply(vector, idxRow, rows[idxRow].get(idxColumn, type), type));
    private static final NullableDecimal256Holder nullDecimal256 = new NullableDecimal256Holder();
    private static final ConvertTo<org.apache.arrow.vector.FieldVector, Integer, Object, DataType> toDecimal256Object = (vector, row, value, type) -> {
        if (value == null) {
            TypeConversionHelper.<Decimal256Vector>cast(vector).setSafe(row, ArrowConversion.nullDecimal256);
        } else {
            TypeConversionHelper.<Decimal256Vector>cast(vector).setSafe(row, value instanceof Decimal decimal
                    ? decimal.toJavaBigDecimal() : BigDecimal.valueOf(Double.parseDouble(value.toString())));
        }
    };
    //convert STRING to arrow VarCharVector
    private static final ConvertTo<org.apache.arrow.vector.FieldVector, InternalRow[], Integer, DataType> toVarChar = (vector, rows, idxColumn, type) -> IntStream.range(0, rows.length).forEach(idxRow -> ArrowConversion.toVarCharObject.apply(vector, idxRow, rows[idxRow].get(idxColumn, type), type));
    private static final NullableVarCharHolder nullVarChar = new NullableVarCharHolder();
    private static final ConvertTo<org.apache.arrow.vector.FieldVector, Integer, Object, DataType> toVarCharObject = (vector, row, value, type) -> {
        if (value == null) {
            TypeConversionHelper.<VarCharVector>cast(vector).setSafe(row, ArrowConversion.nullVarChar);
        } else {
            TypeConversionHelper.<VarCharVector>cast(vector).setSafe(row, new org.apache.arrow.vector.util.Text(value.toString()));
        }
    };
    //convert STRING to arrow LargeVarCharVector
    private static final ConvertTo<org.apache.arrow.vector.FieldVector, InternalRow[], Integer, DataType> toLargeVarChar = (vector, rows, idxColumn, type) -> IntStream.range(0, rows.length).forEach(idxRow -> ArrowConversion.toLargeVarCharObject.apply(vector, idxRow, rows[idxRow].get(idxColumn, type), type));
    private static final NullableLargeVarCharHolder nullLargeVarChar = new NullableLargeVarCharHolder();
    private static final ConvertTo<org.apache.arrow.vector.FieldVector, Integer, Object, DataType> toLargeVarCharObject = (vector, row, value, type) -> {
        if (value == null) {
            TypeConversionHelper.<LargeVarCharVector>cast(vector).setSafe(row, ArrowConversion.nullLargeVarChar);
        } else {
            TypeConversionHelper.<LargeVarCharVector>cast(vector).setSafe(row, new org.apache.arrow.vector.util.Text(value.toString()));
        }
    };
    //convert BOOLEAN to arrow BitVector
    private static final ConvertTo<org.apache.arrow.vector.FieldVector, InternalRow[], Integer, DataType> toBit = (vector, rows, idxColumn, type) -> {
        if (type == DataTypes.BooleanType) {
            IntStream.range(0, rows.length).forEach(idxRow -> ArrowConversion.toBitObject.apply(vector, idxRow, rows[idxRow].get(idxColumn, type), type));
        } else {
            throw new IllegalArgumentException("The data cannot be converted to arrow Bit.");
        }
    };
    private static final NullableBitHolder nullBit = new NullableBitHolder();
    private static final ConvertTo<org.apache.arrow.vector.FieldVector, Integer, Object, DataType> toBitObject = (vector, row, value, type) -> {
        if (value == null) {
            TypeConversionHelper.<BitVector>cast(vector).setSafe(row, ArrowConversion.nullBit);
        } else {
            TypeConversionHelper.<BitVector>cast(vector).setSafe(row, (Boolean) value ? 1 : 0);
        }
    };
    //convert DATE to arrow DateDayVector
    private static final ConvertTo<org.apache.arrow.vector.FieldVector, InternalRow[], Integer, DataType> toDateDay = (vector, rows, idxColumn, type) -> {
        if (type == DataTypes.DateType || type == DataTypes.TimestampType) {
            IntStream.range(0, rows.length).forEach(idxRow -> ArrowConversion.toDateDayObject.apply(vector, idxRow, rows[idxRow].get(idxColumn, type), type));
        } else {
            throw new IllegalArgumentException("The data cannot be converted to arrow DateDay.");
        }
    };
    private static final NullableDateDayHolder nullDateDay = new NullableDateDayHolder();
    private static final ConvertTo<org.apache.arrow.vector.FieldVector, Integer, Object, DataType> toDateDayObject = (vector, row, value, type) -> {
        if (value == null) {
            TypeConversionHelper.<DateDayVector>cast(vector).setSafe(row, ArrowConversion.nullDateDay);
        } else if (type == DataTypes.DateType) {
            TypeConversionHelper.<DateDayVector>cast(vector).setSafe(row, (value instanceof Number) ? (Integer) value : Integer.parseInt(value.toString()));
        } else if (type == DataTypes.TimestampType) {
            TypeConversionHelper.<DateDayVector>cast(vector).setSafe(row, DateTimeUtils.microsToDays((value instanceof Number) ? (Long) value : Long.parseLong(value.toString()), ZoneId.systemDefault()));
        }
    };
    //convert DATE to arrow DateMilliVector
    private static final ConvertTo<org.apache.arrow.vector.FieldVector, InternalRow[], Integer, DataType> toDateMilli = (vector, rows, idxColumn, type) -> {
        if (type == DataTypes.DateType || type == DataTypes.TimestampType) {
            IntStream.range(0, rows.length).forEach(idxRow -> ArrowConversion.toDateMilliObject.apply(vector, idxRow, rows[idxRow].get(idxColumn, type), type));
        } else {
            throw new IllegalArgumentException("The data cannot be converted to arrow DateMilli.");
        }
    };
    private static final NullableDateMilliHolder nullDateMilli = new NullableDateMilliHolder();
    private static final ConvertTo<org.apache.arrow.vector.FieldVector, Integer, Object, DataType> toDateMilliObject = (vector, row, value, type) -> {
        if (value == null) {
            TypeConversionHelper.<DateMilliVector>cast(vector).setSafe(row, ArrowConversion.nullDateMilli);
        } else {
            LocalDateTime ldt;
            if (type == DataTypes.DateType) {
                int days = value instanceof Number number
                        ? (Integer) number : Integer.parseInt(value.toString());
                ldt = LocalDateTime.of(DateTimeUtils.daysToLocalDate(days),
                        LocalTime.of(0, 0));
            } else if (type == DataTypes.TimestampType) {
                long micros = value instanceof Number number
                        ? (Long) number : Long.parseLong(value.toString());
                ldt = DateTimeUtils.microsToLocalDateTime(micros);
            } else {
                ldt = LocalDateTime.now(ZoneId.systemDefault());
            }
            TypeConversionHelper.<DateMilliVector>cast(vector).setSafe(row, Timestamp.valueOf(ldt).getTime());
        }
    };
    //convert TIME to arrow TimeSecVector
    private static final ConvertTo<org.apache.arrow.vector.FieldVector, InternalRow[], Integer, DataType> toTimeSec = (vector, rows, idxColumn, type) -> {
        if (type == DataTypes.TimestampType || type == DataTypes.StringType) {
            IntStream.range(0, rows.length).forEach(idxRow -> ArrowConversion.toTimeSecObject.apply(vector, idxRow, rows[idxRow].get(idxColumn, type), type));
        } else {
            throw new IllegalArgumentException("The data cannot be converted to arrow TimeSec.");
        }
    };
    private static final NullableTimeSecHolder nullTimeSec = new NullableTimeSecHolder();
    private static final ConvertTo<org.apache.arrow.vector.FieldVector, Integer, Object, DataType> toTimeSecObject = (vector, row, value, type) -> {
        if (value == null) {
            TypeConversionHelper.<TimeSecVector>cast(vector).setSafe(row, ArrowConversion.nullTimeSec);
        } else if (type == DataTypes.TimestampType) {
            TypeConversionHelper.<TimeSecVector>cast(vector).setSafe(row, (int) (TypeConversionHelper.microsToEpochNanos.apply((value instanceof Number) ? (Long) value : Long.parseLong(value.toString())) / 1000000000L));
        } else if (type == DataTypes.StringType) {
            TypeConversionHelper.<TimeSecVector>cast(vector).setSafe(row, (int) (TypeConversionHelper.timestrToNanos.applyAsLong(value.toString()) / 1000000000L));
        }
    };
    //convert TIME to arrow TimeMilliVector
    private static final ConvertTo<org.apache.arrow.vector.FieldVector, InternalRow[], Integer, DataType> toTimeMilli = (vector, rows, idxColumn, type) -> {
        if (type == DataTypes.TimestampType || type == DataTypes.StringType) {
            IntStream.range(0, rows.length).forEach(idxRow -> ArrowConversion.toTimeMilliObject.apply(vector, idxRow, rows[idxRow].get(idxColumn, type), type));
        } else {
            throw new IllegalArgumentException("The data cannot be converted to arrow TimeMilli.");
        }
    };
    private static final NullableTimeMilliHolder nullTimeMilli = new NullableTimeMilliHolder();
    private static final ConvertTo<org.apache.arrow.vector.FieldVector, Integer, Object, DataType> toTimeMilliObject = (vector, row, value, type) -> {
        if (value == null) {
            TypeConversionHelper.<TimeMilliVector>cast(vector).setSafe(row, ArrowConversion.nullTimeMilli);
        } else if (type == DataTypes.TimestampType) {
            TypeConversionHelper.<TimeMilliVector>cast(vector).setSafe(row, (int) (TypeConversionHelper.microsToEpochNanos.apply((value instanceof Number) ? (Long) value : Long.parseLong(value.toString())) / 1000000L));
        } else if (type == DataTypes.StringType) {
            TypeConversionHelper.<TimeMilliVector>cast(vector).setSafe(row, (int) (TypeConversionHelper.timestrToNanos.applyAsLong(value.toString()) / 1000000L));
        }
    };
    //convert TIME arrow TimeMicroVector
    private static final ConvertTo<org.apache.arrow.vector.FieldVector, InternalRow[], Integer, DataType> toTimeMicro = (vector, rows, idxColumn, type) -> {
        if (type == DataTypes.TimestampType || type == DataTypes.StringType) {
            IntStream.range(0, rows.length).forEach(idxRow -> ArrowConversion.toTimeMicroObject.apply(vector, idxRow, rows[idxRow].get(idxColumn, type), type));
        } else {
            throw new IllegalArgumentException("The data cannot be converted to arrow TimeMicro.");
        }
    };
    private static final NullableTimeMicroHolder nullTimeMicro = new NullableTimeMicroHolder();
    private static final ConvertTo<org.apache.arrow.vector.FieldVector, Integer, Object, DataType> toTimeMicroObject = (vector, row, value, type) -> {
        if (value == null) {
            TypeConversionHelper.<TimeMicroVector>cast(vector).setSafe(row, ArrowConversion.nullTimeMicro);
        } else if (type == DataTypes.TimestampType) {
           TypeConversionHelper.<TimeMicroVector>cast(vector).setSafe(row, TypeConversionHelper.microsToEpochNanos.apply((value instanceof Number) ? (Long) value : Long.parseLong(value.toString())) / 1000L);
        } else if (type == DataTypes.StringType) {
            TypeConversionHelper.<TimeMicroVector>cast(vector).setSafe(row, TypeConversionHelper.timestrToNanos.applyAsLong(value.toString()) / 1000L);
        }
    };
    //convert TIME to arrow TimeNanoVector
    private static final ConvertTo<org.apache.arrow.vector.FieldVector, InternalRow[], Integer, DataType> toTimeNano = (vector, rows, idxColumn, type) -> {
        if (type == DataTypes.TimestampType || type == DataTypes.StringType) {
            IntStream.range(0, rows.length).forEach(idxRow -> ArrowConversion.toTimeNanoObject.apply(vector, idxRow, rows[idxRow].get(idxColumn, type), type));
        } else {
            throw new IllegalArgumentException("The data cannot be converted to arrow TimeNano.");
        }
    };
    private static final NullableTimeNanoHolder nullTimeNano = new NullableTimeNanoHolder();
    private static final ConvertTo<org.apache.arrow.vector.FieldVector, Integer, Object, DataType> toTimeNanoObject = (vector, row, value, type) -> {
        if (value == null) {
            TypeConversionHelper.<TimeNanoVector>cast(vector).setSafe(row, ArrowConversion.nullTimeNano);
        } else if (type == DataTypes.TimestampType) {
            TypeConversionHelper.<TimeNanoVector>cast(vector).setSafe(row, TypeConversionHelper.microsToEpochNanos.apply((value instanceof Number) ? (Long) value : Long.parseLong(value.toString())));
        } else if (type == DataTypes.StringType) {
            TypeConversionHelper.<TimeNanoVector>cast(vector).setSafe(row, TypeConversionHelper.timestrToNanos.applyAsLong(value.toString()));
        }
    };
    //convert TIMESTAMP to arrow TimeStampMicroVector
    private static final ConvertTo<org.apache.arrow.vector.FieldVector, InternalRow[], Integer, DataType> toTimeStampMicro = (vector, rows, idxColumn, type) -> {
        if (type == DataTypes.TimestampType || type == DataTypes.DateType) {
            IntStream.range(0, rows.length).forEach(idxRow -> ArrowConversion.toTimeStampMicroObject.apply(vector, idxRow, rows[idxRow].get(idxColumn, type), type));
        } else {
            throw new IllegalArgumentException("The data cannot be converted to arrow TimeStampMicro.");
        }
    };
    private static final NullableTimeStampMicroHolder nullTimeStampMicro = new NullableTimeStampMicroHolder();
    private static final ConvertTo<org.apache.arrow.vector.FieldVector, Integer, Object, DataType> toTimeStampMicroObject = (vector, row, value, type) -> {
        if (value == null) {
            TypeConversionHelper.<TimeStampMicroVector>cast(vector).setSafe(row, ArrowConversion.nullTimeStampMicro);
        } else if (type == DataTypes.TimestampType) {
            TypeConversionHelper.<TimeStampMicroVector>cast(vector).setSafe(row, ((value instanceof Number) ? (Long) value : Long.parseLong(value.toString())));
        } else if (type == DataTypes.DateType) {
            TypeConversionHelper.<TimeStampMicroVector>cast(vector).setSafe(row, TypeConversionHelper.daysToMicros.apply((value instanceof Number) ? (Integer) value : Integer.parseInt(value.toString()), ZoneId.systemDefault()));
        }
    };
    //convert TIMESTAMP to arrow TimeStampMicroTZVector
    private static final ConvertTo<org.apache.arrow.vector.FieldVector, InternalRow[], Integer, DataType> toTimeStampMicroTZ = (vector, rows, idxColumn, type) -> {
        if (type == DataTypes.TimestampType || type == DataTypes.DateType) {
            IntStream.range(0, rows.length).forEach(idxRow -> ArrowConversion.toTimeStampMicroTZObject.apply(vector, idxRow, rows[idxRow].get(idxColumn, type), type));
        } else {
            throw new IllegalArgumentException("The data cannot be converted to arrow TimeStampMicroTZ.");
        }
    };
    private static final NullableTimeStampMicroTZHolder nullTimeStampMicroTZ = new NullableTimeStampMicroTZHolder();
    private static final ConvertTo<org.apache.arrow.vector.FieldVector, Integer, Object, DataType> toTimeStampMicroTZObject = (vector, row, value, type) -> {
        if (value == null) {
            TypeConversionHelper.<TimeStampMicroTZVector>cast(vector).setSafe(row, ArrowConversion.nullTimeStampMicroTZ);
        } else if (type == DataTypes.TimestampType) {
            TypeConversionHelper.<TimeStampMicroTZVector>cast(vector).setSafe(row, DateTimeUtils.convertTz((value instanceof Number) ? (Long) value : Long.parseLong(value.toString()), ZoneId.systemDefault(), ZoneId.of(TypeConversionHelper.<TimeStampMicroTZVector>cast(vector).getTimeZone())));
        } else if (type == DataTypes.DateType) {
            TypeConversionHelper.<TimeStampMicroTZVector>cast(vector).setSafe(row, TypeConversionHelper.daysToMicros.apply((value instanceof Number) ? (Integer) value : Integer.parseInt(value.toString()), ZoneId.of(TypeConversionHelper.<TimeStampMicroTZVector>cast(vector).getTimeZone())));
        }
    };
    //convert TIMESTAMP to arrow TimeStampMilliVector
    private static final ConvertTo<org.apache.arrow.vector.FieldVector, InternalRow[], Integer, DataType> toTimeStampMilli = (vector, rows, idxColumn, type) -> {
        if (type == DataTypes.TimestampType || type == DataTypes.DateType) {
            IntStream.range(0, rows.length).forEach(idxRow -> ArrowConversion.toTimeStampMilliObject.apply(vector, idxRow, rows[idxRow].get(idxColumn, type), type));
        } else {
            throw new IllegalArgumentException("The data cannot be converted to arrow TimeStampMilli.");
        }
    };
    private static final NullableTimeStampMilliHolder nullTimeStampMilli = new NullableTimeStampMilliHolder();
    private static final ConvertTo<org.apache.arrow.vector.FieldVector, Integer, Object, DataType> toTimeStampMilliObject = (vector, row, value, type) -> {
        if (value == null) {
            TypeConversionHelper.<TimeStampMilliVector>cast(vector).setSafe(row, ArrowConversion.nullTimeStampMilli);
        } else if (type == DataTypes.TimestampType) {
            TypeConversionHelper.<TimeStampMilliVector>cast(vector).setSafe(row, TypeConversionHelper.microsToMillis.applyAsLong(((value instanceof Number) ? (Long) value : Long.parseLong(value.toString()))));
        } else if (type == DataTypes.DateType) {
            TypeConversionHelper.<TimeStampMilliVector>cast(vector).setSafe(row, TypeConversionHelper.microsToMillis.applyAsLong(TypeConversionHelper.daysToMicros.apply((value instanceof Number) ? (Integer) value : Integer.parseInt(value.toString()), ZoneId.systemDefault())));
        }
    };
    //convert TIMESTAMP to arrow TimeStampMilliTZVector
    private static final ConvertTo<org.apache.arrow.vector.FieldVector, InternalRow[], Integer, DataType> toTimeStampMilliTZ = (vector, rows, idxColumn, type) -> {
        if (type == DataTypes.TimestampType || type == DataTypes.DateType) {
            IntStream.range(0, rows.length).forEach(idxRow -> ArrowConversion.toTimeStampMilliTZObject.apply(vector, idxRow, rows[idxRow].get(idxColumn, type), type));
        } else {
            throw new IllegalArgumentException("The data cannot be converted to arrow TimeStampMilliTZ.");
        }
    };
    private static final NullableTimeStampMilliTZHolder nullTimeStampMilliTZ = new NullableTimeStampMilliTZHolder();
    private static final ConvertTo<org.apache.arrow.vector.FieldVector, Integer, Object, DataType> toTimeStampMilliTZObject = (vector, row, value, type) -> {
        if (value == null) {
            TypeConversionHelper.<TimeStampMilliTZVector>cast(vector).setSafe(row, ArrowConversion.nullTimeStampMilliTZ);
        } else if (type == DataTypes.TimestampType) {
            TypeConversionHelper.<TimeStampMilliTZVector>cast(vector).setSafe(row, TypeConversionHelper.microsToMillis.applyAsLong(DateTimeUtils.convertTz((value instanceof Number) ? (Long) value : Long.parseLong(value.toString()), ZoneId.systemDefault(), ZoneId.of(TypeConversionHelper.<TimeStampMilliTZVector>cast(vector).getTimeZone()))));
        } else if (type == DataTypes.DateType) {
            TypeConversionHelper.<TimeStampMilliTZVector>cast(vector).setSafe(row, TypeConversionHelper.microsToMillis.applyAsLong(TypeConversionHelper.daysToMicros.apply((value instanceof Number) ? (Integer) value : Integer.parseInt(value.toString()), ZoneId.of(TypeConversionHelper.<TimeStampMilliTZVector>cast(vector).getTimeZone()))));
        }
    };
    //convert TIMESTAMP to arrow TimeStampSecVector
    private static final ConvertTo<org.apache.arrow.vector.FieldVector, InternalRow[], Integer, DataType> toTimeStampSec = (vector, rows, idxColumn, type) -> {
        if (type == DataTypes.TimestampType || type == DataTypes.DateType) {
            IntStream.range(0, rows.length).forEach(idxRow -> ArrowConversion.toTimeStampSecObject.apply(vector, idxRow, rows[idxRow].get(idxColumn, type), type));
        } else {
            throw new IllegalArgumentException("The data cannot be converted to arrow TimeStampSec.");
        }
    };
    private static final NullableTimeStampSecHolder nullTimeStampSec = new NullableTimeStampSecHolder();
    private static final ConvertTo<org.apache.arrow.vector.FieldVector, Integer, Object, DataType> toTimeStampSecObject = (vector, row, value, type) -> {
        if (value == null) {
            TypeConversionHelper.<TimeStampSecVector>cast(vector).setSafe(row, ArrowConversion.nullTimeStampSec);
        } else if (type == DataTypes.TimestampType) {
            TypeConversionHelper.<TimeStampSecVector>cast(vector).setSafe(row, TypeConversionHelper.microsToSecs.applyAsLong(((value instanceof Number) ? (Long) value : Long.parseLong(value.toString()))));
        } else if (type == DataTypes.DateType) {
            TypeConversionHelper.<TimeStampSecVector>cast(vector).setSafe(row, TypeConversionHelper.microsToSecs.applyAsLong(TypeConversionHelper.daysToMicros.apply((value instanceof Number) ? (Integer) value : Integer.parseInt(value.toString()), ZoneId.systemDefault())));
        }
    };
    //convert TIMESTAMP to arrow TimeStampSecTZVector
    private static final ConvertTo<org.apache.arrow.vector.FieldVector, InternalRow[], Integer, DataType> toTimeStampSecTZ = (vector, rows, idxColumn, type) -> {
        if (type == DataTypes.TimestampType || type == DataTypes.DateType) {
            IntStream.range(0, rows.length).forEach(idxRow -> ArrowConversion.toTimeStampSecTZObject.apply(vector, idxRow, rows[idxRow].get(idxColumn, type), type));
        } else {
            throw new IllegalArgumentException("The data cannot be converted to arrow TimeStampSecTZ.");
        }
    };
    private static final NullableTimeStampSecTZHolder nullTimeStampSecTZ = new NullableTimeStampSecTZHolder();
    private static final ConvertTo<org.apache.arrow.vector.FieldVector, Integer, Object, DataType> toTimeStampSecTZObject = (vector, row, value, type) -> {
        if (value == null) {
            TypeConversionHelper.<TimeStampSecTZVector>cast(vector).setSafe(row, ArrowConversion.nullTimeStampSecTZ);
        } else if (type == DataTypes.TimestampType) {
            TypeConversionHelper.<TimeStampSecTZVector>cast(vector).setSafe(row, TypeConversionHelper.microsToSecs.applyAsLong(DateTimeUtils.convertTz((value instanceof Number) ? (Long) value : Long.parseLong(value.toString()), ZoneId.systemDefault(), ZoneId.of(TypeConversionHelper.<TimeStampSecTZVector>cast(vector).getTimeZone()))));
        } else if (type == DataTypes.DateType) {
            TypeConversionHelper.<TimeStampSecTZVector>cast(vector).setSafe(row, TypeConversionHelper.microsToSecs.applyAsLong(TypeConversionHelper.daysToMicros.apply((value instanceof Number) ? (Integer) value : Integer.parseInt(value.toString()), ZoneId.of(TypeConversionHelper.<TimeStampSecTZVector>cast(vector).getTimeZone()))));
        }
    };
    //convert TIMESTAMP to arrow TimeStampNanoVector
    private static final ConvertTo<org.apache.arrow.vector.FieldVector, InternalRow[], Integer, DataType> toTimeStampNano = (vector, rows, idxColumn, type) -> {
        if (type == DataTypes.TimestampType || type == DataTypes.DateType) {
            IntStream.range(0, rows.length).forEach(idxRow -> ArrowConversion.toTimeStampNanoObject.apply(vector, idxRow, rows[idxRow].get(idxColumn, type), type));
        } else {
            throw new IllegalArgumentException("The data cannot be converted to arrow TimeStampNano.");
        }
    };
    private static final NullableTimeStampNanoHolder nullTimeStampNano = new NullableTimeStampNanoHolder();
    private static final ConvertTo<org.apache.arrow.vector.FieldVector, Integer, Object, DataType> toTimeStampNanoObject = (vector, row, value, type) -> {
        if (value == null) {
            TypeConversionHelper.<TimeStampNanoVector>cast(vector).setSafe(row, ArrowConversion.nullTimeStampNano);
        } else if (type == DataTypes.TimestampType) {
            TypeConversionHelper.<TimeStampNanoVector>cast(vector).setSafe(row, TypeConversionHelper.microsToNanos.applyAsLong(((value instanceof Number) ? (Long) value : Long.parseLong(value.toString()))));
        } else if (type == DataTypes.DateType) {
            TypeConversionHelper.<TimeStampNanoVector>cast(vector).setSafe(row, TypeConversionHelper.microsToNanos.applyAsLong(TypeConversionHelper.daysToMicros.apply((value instanceof Number) ? (Integer) value : Integer.parseInt(value.toString()), ZoneId.systemDefault())));
        }
    };
    //convert TIMESTAMP to arrow TimeStampNanoTZVector
    private static final ConvertTo<org.apache.arrow.vector.FieldVector, InternalRow[], Integer, DataType> toTimeStampNanoTZ = (vector, rows, idxColumn, type) -> {
        if (type == DataTypes.TimestampType || type == DataTypes.DateType) {
            IntStream.range(0, rows.length).forEach(idxRow -> ArrowConversion.toTimeStampNanoTZObject.apply(vector, idxRow, rows[idxRow].get(idxColumn, type), type));
        } else {
            throw new IllegalArgumentException("The data cannot be converted to arrow TimeStampNanoTZ.");
        }
    };
    private static final NullableTimeStampNanoTZHolder nullTimeStampNanoTZ = new NullableTimeStampNanoTZHolder();
    private static final ConvertTo<org.apache.arrow.vector.FieldVector, Integer, Object, DataType> toTimeStampNanoTZObject = (vector, row, value, type) -> {
        if (value == null) {
            TypeConversionHelper.<TimeStampNanoTZVector>cast(vector).setSafe(row, ArrowConversion.nullTimeStampNanoTZ);
        } else if (type == DataTypes.TimestampType) {
            TypeConversionHelper.<TimeStampNanoTZVector>cast(vector).setSafe(row, TypeConversionHelper.microsToNanos.applyAsLong(DateTimeUtils.convertTz((value instanceof Number) ? (Long) value : Long.parseLong(value.toString()), ZoneId.systemDefault(), ZoneId.of(TypeConversionHelper.<TimeStampNanoTZVector>cast(vector).getTimeZone()))));
        } else {
            TypeConversionHelper.<TimeStampNanoTZVector>cast(vector).setSafe(row, TypeConversionHelper.microsToNanos.applyAsLong(TypeConversionHelper.daysToMicros.apply((value instanceof Number) ? (Integer) value : Integer.parseInt(value.toString()), ZoneId.of(TypeConversionHelper.<TimeStampNanoTZVector>cast(vector).getTimeZone()))));
        }
    };
    //convert DURATION_DAY_TIME to arrow DurationVector
    private static final ConvertTo<org.apache.arrow.vector.FieldVector, InternalRow[], Integer, DataType> toDuration = (vector, rows, idxColumn, type) -> {
        if (type instanceof DayTimeIntervalType) {
            IntStream.range(0, rows.length).forEach(idxRow -> ArrowConversion.toDurationObject.apply(vector, idxRow, rows[idxRow].get(idxColumn, type), type));
        } else {
            throw new IllegalArgumentException("The data cannot be converted to arrow Duration.");
        }
    };
    private static final NullableDurationHolder nullDuration = new NullableDurationHolder();
    private static final ConvertTo<org.apache.arrow.vector.FieldVector, Integer, Object, DataType> toDurationObject = (vector, row, value, type) -> {
        if (value == null) {
            TypeConversionHelper.<DurationVector>cast(vector).setSafe(row, ArrowConversion.nullDuration);
        } else {
            TypeConversionHelper.<DurationVector>cast(vector).setSafe(row, (value instanceof Number) ? (Long) value : Long.parseLong(value.toString()));
        }
    };
    //convert PERIOD_YEAR_MONTH to arrow IntervalYearVector
    private static final ConvertTo<org.apache.arrow.vector.FieldVector, InternalRow[], Integer, DataType> toIntervalYear = (vector, rows, idxColumn, type) -> {
        if (type instanceof YearMonthIntervalType) {
            IntStream.range(0, rows.length).forEach(idxRow -> ArrowConversion.toIntervalYearObject.apply(vector, idxRow, rows[idxRow].get(idxColumn, type), type));
        } else {
            throw new IllegalArgumentException("The data cannot be converted to arrow IntervalYear.");
        }
    };
    private static final NullableIntervalYearHolder nullIntervalYear = new NullableIntervalYearHolder();
    private static final ConvertTo<org.apache.arrow.vector.FieldVector, Integer, Object, DataType> toIntervalYearObject = (vector, row, value, type) -> {
        if (value == null) {
            TypeConversionHelper.<IntervalYearVector>cast(vector).setSafe(row, ArrowConversion.nullIntervalYear);
        } else {
            TypeConversionHelper.<IntervalYearVector>cast(vector).setSafe(row, (value instanceof Number) ? (Integer) value : Integer.parseInt(value.toString()));
        }
    };
    //convert DURATION_DAY_TIME to arrow IntervalDayVector
    private static final ConvertTo<org.apache.arrow.vector.FieldVector, InternalRow[], Integer, DataType> toIntervalDay = (vector, rows, idxColumn, type) -> {
        if (type instanceof DayTimeIntervalType) {
            IntStream.range(0, rows.length).forEach(idxRow -> ArrowConversion.toIntervalDayObject.apply(vector, idxRow, rows[idxRow].get(idxColumn, type), type));
        } else {
            throw new IllegalArgumentException("The data cannot be converted to arrow IntervalDay.");
        }
    };
    private static final NullableIntervalDayHolder nullIntervalDay = new NullableIntervalDayHolder();
    private static final ConvertTo<org.apache.arrow.vector.FieldVector, Integer, Object, DataType> toIntervalDayObject = (vector, row, value, type) -> {
        if (value == null) {
            TypeConversionHelper.<IntervalDayVector>cast(vector).setSafe(row, ArrowConversion.nullIntervalDay);
        } else {
            long micros = (value instanceof Number) ? (Long) value : Long.parseLong(value.toString());
            TypeConversionHelper.<IntervalDayVector>cast(vector).setSafe(row, IntervalUtils.getDays(micros), (int) (micros % 1000L));
        }
    };
    //convert StructType to arrow StructVector
    private static final ConvertTo<org.apache.arrow.vector.FieldVector, InternalRow[], Integer, DataType> toStruct = (vector, rows, idxColumn, type) -> {
        if (vector instanceof StructVector && type instanceof StructType structType) {
            IntStream.range(0, rows.length).forEach(idxRow -> ArrowConversion.toStructObject.apply(vector, idxRow, rows[idxRow].get(idxColumn, structType), structType));
        } else if (vector instanceof StructVector && type instanceof MapType mapType) {
            IntStream.range(0, rows.length).forEach(idxRow -> ArrowConversion.toStructMap.apply(vector, idxRow, rows[idxRow].get(idxColumn, mapType), mapType));
        } else {
            throw new IllegalArgumentException("The data cannot be converted to arrow Struct.");
        }
    };
    private static final ConvertTo<org.apache.arrow.vector.FieldVector, Integer, Object, DataType> toStructObject = (vector, row, value, type) -> {
        if (value == null) {
            TypeConversionHelper.<StructVector>cast(vector).setNull(row);
        } else if (type instanceof StructType structType) {
            org.apache.arrow.vector.FieldVector[] vectorChildren = TypeConversionHelper.<StructVector>cast(vector).getChildrenFromFields().toArray(new org.apache.arrow.vector.FieldVector[0]);
            DataType[] dataTypes = Arrays.stream(structType.fields()).map(StructField::dataType).toList().toArray(new DataType[0]);
            if (vectorChildren.length == dataTypes.length) {
                UnsafeRow rowsChildren = (UnsafeRow) value;
                IntStream.range(0, vectorChildren.length).forEach(idx -> ArrowConversion.getOrCreate().populateObject(vectorChildren[idx], 0, rowsChildren.get(idx, dataTypes[idx]), dataTypes[idx]));
            } else {
                throw new IllegalArgumentException("The data cannot be converted to arrow Struct.");
            }
        }
    };
    private static final ConvertTo<org.apache.arrow.vector.FieldVector, Integer, Object, DataType> toStructMap = (vector, row, value, type) -> {
        if (value == null) {
            TypeConversionHelper.<StructVector>cast(vector).setNull(row);
        } else {
            boolean populated = false;
            if (type instanceof MapType mapType) {
                org.apache.arrow.vector.FieldVector[] vectorChildren = TypeConversionHelper.<StructVector>cast(vector).getChildrenFromFields().toArray(new org.apache.arrow.vector.FieldVector[0]);
                if (vectorChildren.length == 1 && vectorChildren[0] instanceof ListVector && vectorChildren[0].getName().equals("map")) {
                    org.apache.arrow.vector.FieldVector dataVector = TypeConversionHelper.<ListVector>cast(vectorChildren[0]).getDataVector();
                    if (dataVector instanceof StructVector) {
                        org.apache.arrow.vector.FieldVector[] valueChildren = TypeConversionHelper.<StructVector>cast(dataVector).getChildrenFromFields().toArray(new org.apache.arrow.vector.FieldVector[0]);
                        if (valueChildren.length == 2 && valueChildren[0].getName().equals("key") && valueChildren[1].getName().equals("value")) {
                            DataType keyType = mapType.keyType();
                            DataType valueType = mapType.valueType();
                            UnsafeMapData data = (UnsafeMapData) value;
                            UnsafeArrayData keyData = data.keyArray();
                            UnsafeArrayData valueData = data.valueArray();
                            IntStream.range(0, data.numElements()).forEach(idx -> {
                                ArrowConversion.getOrCreate().populateObject(valueChildren[0], idx, keyData.get(idx, keyType), keyType);
                                ArrowConversion.getOrCreate().populateObject(valueChildren[1], idx, valueData.get(idx, valueType), valueType);
                            });
                            populated = true;
                        }
                    }
                }
            }
            if (!populated) {
                throw new IllegalArgumentException("The data cannot be converted to arrow Struct.");
            }
        }
    };
    //convert ArrayType to arrow ListVector
    private static final ConvertTo<org.apache.arrow.vector.FieldVector, InternalRow[], Integer, DataType> toList = (vector, rows, idxColumn, type) -> {
        if (vector instanceof ListVector && type instanceof ArrayType arrayType) {
            IntStream.range(0, rows.length).forEach(idxRow -> ArrowConversion.toListObject.apply(vector, idxRow, rows[idxRow].get(idxColumn, arrayType), arrayType));
        } else {
            throw new IllegalArgumentException("The data cannot be converted to arrow List.");
        }
    };
    private static final ConvertTo<org.apache.arrow.vector.FieldVector, Integer, Object, DataType> toListObject = (vector, row, value, type) -> {
        if (value == null) {
            TypeConversionHelper.<ListVector>cast(vector).setNull(row);
        } else if (type instanceof ArrayType arrayType) {
            org.apache.arrow.vector.FieldVector dataVector = TypeConversionHelper.<ListVector>cast(vector).getDataVector();
            DataType dataType = arrayType.elementType();
            UnsafeArrayData data = (UnsafeArrayData) value;
            IntStream.range(0, data.numElements()).forEach(idx -> ArrowConversion.getOrCreate().populateObject(dataVector, idx, data.get(idx, dataType), dataType));
        } else {
            throw new IllegalArgumentException("The data cannot be converted to arrow List.");
        }
    };
    //convert MapType to arrow MapVector
    private static final ConvertTo<org.apache.arrow.vector.FieldVector, InternalRow[], Integer, DataType> toMap = (vector, rows, idxColumn, type) -> {
        if (vector instanceof MapVector && type instanceof MapType mapType) {
            IntStream.range(0, rows.length).forEach(idxRow -> ArrowConversion.toMapObject.apply(vector, idxRow, rows[idxRow].get(idxColumn, mapType), mapType));
        } else {
            throw new IllegalArgumentException("The data cannot be converted to arrow Map.");
        }
    };
    private static final ConvertTo<org.apache.arrow.vector.FieldVector, Integer, Object, DataType> toMapObject = (vector, row, value, type) -> {
        if (value == null) {
            TypeConversionHelper.<MapVector>cast(vector).setNull(row);
        } else if (type instanceof MapType mapType) {
            org.apache.arrow.vector.FieldVector[] valueChildren = TypeConversionHelper.<MapVector>cast(vector).getChildrenFromFields().toArray(new org.apache.arrow.vector.FieldVector[0]);
            if (valueChildren.length == 2) {
                DataType keyType = mapType.keyType();
                DataType valueType = mapType.valueType();
                UnsafeMapData data = (UnsafeMapData) value;
                UnsafeArrayData keyData = data.keyArray();
                UnsafeArrayData valueData = data.valueArray();
                IntStream.range(0, data.numElements()).forEach(idx -> {
                    ArrowConversion.getOrCreate().populateObject(valueChildren[0], idx, keyData.get(idx, keyType), keyType);
                    ArrowConversion.getOrCreate().populateObject(valueChildren[1], idx, valueData.get(idx, valueType), valueType);
                });
            } else {
                throw new IllegalArgumentException("The data cannot be converted to arrow Map.");
            }
        }
    };

    //the singleton instance
    private static ArrowConversion inst = null;
    /**
     * Get or create an instance of Vector
     * @return - the singleton instance of Vector
     */
    public static synchronized ArrowConversion getOrCreate() {
        if (ArrowConversion.inst == null) {
            ArrowConversion.inst = new ArrowConversion();
        }
        return ArrowConversion.inst;
    }

    /**
     * Returns the singleton instance on deserialization.
     * @return singleton ArrowConversion instance
     */
    private Object readResolve() {
        return getOrCreate();
    }
}
