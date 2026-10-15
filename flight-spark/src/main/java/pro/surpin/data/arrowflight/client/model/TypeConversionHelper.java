package pro.surpin.data.arrowflight.client.model;

import com.google.common.collect.Streams;
import org.apache.arrow.vector.*;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.MapVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.util.JsonStringHashMap;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.util.ArrayBasedMapData;
import org.apache.spark.sql.catalyst.util.ArrayData;
import org.apache.spark.sql.catalyst.util.DateTimeUtils;
import org.apache.spark.sql.catalyst.util.IntervalUtils;
import org.apache.spark.sql.types.Decimal;
import org.apache.spark.unsafe.types.UTF8String;
import scala.collection.JavaConverters;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.time.*;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.BinaryOperator;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.IntUnaryOperator;
import java.util.function.LongUnaryOperator;
import java.util.function.ToLongFunction;

/**
 * Converts values between Arrow, Spark, and Java representations.
 */
public class TypeConversionHelper {
    private static final String KEY_FIELD_NAME = "key";
    private static final String VALUE_FIELD_NAME = "value";

    /**
     * Prevents utility class instantiation.
     */
    private TypeConversionHelper() {
    }

    static final LongUnaryOperator microsToNanos = micros -> TypeConversionHelper.microsToMillis.applyAsLong(micros) * 1000L;
    static final LongUnaryOperator microsToMillis = DateTimeUtils::microsToMillis;
    static final LongUnaryOperator microsToSecs = micros -> TypeConversionHelper.microsToMillis.applyAsLong(micros) / 1000L;

    public static final BiFunction<Integer, ZoneId, Long> daysToMicros = (days, zone) -> DateTimeUtils.daysToMicros(days, ZoneId.systemDefault());
    public static final Function<Long, Long> microsToEpochNanos = (micros) -> {
        Instant t = DateTimeUtils.microsToLocalDateTime(micros).withYear(1970)
                .withMonth(Month.JANUARY.getValue()).withDayOfMonth(1)
                .atZone(ZoneId.systemDefault()).toInstant();
        return t.toEpochMilli() * 1000000L + t.getNano();
    };
    static final ToLongFunction<String> timestrToNanos = ts -> {
        Instant t = LocalDateTime.of(LocalDate.of(1970, Month.JANUARY, 1),
                LocalTime.parse(ts)).atZone(ZoneId.systemDefault()).toInstant();
        return t.toEpochMilli() * 1000000L + t.getNano();
    };
    static final BinaryOperator<Object> o1ElseO2 = (o1, o2) -> (o1 != null) ? o1 : o2;

    //base converters
    static final Function<BigDecimal, Decimal> bigDecimalToDecimal = Decimal::apply;
    static final Function<String, UTF8String> stringToUtf8String = UTF8String::fromString;
    static final IntUnaryOperator dateDayToInt = dd -> DateTimeUtils.fromJavaDate(java.sql.Date.valueOf(LocalDate.ofEpochDay(dd)));
    public static final Function<LocalDateTime, Integer> localDateTimeToInt = (ldt) -> DateTimeUtils.fromJavaDate(java.sql.Date.valueOf(ldt.toLocalDate()));
    public static final Function<LocalDateTime, Long> localDateTimeToLong = (ldt) -> DateTimeUtils.fromJavaTimestamp(Timestamp.valueOf(ldt));
    public static final BiFunction<Long, String, Long> timestampSecTZToLong = (ss, zone) -> DateTimeUtils.fromJavaTimestamp(Timestamp.valueOf(LocalDateTime.from(Instant.ofEpochSecond(ss).atZone(ZoneId.of(zone)))));
    public static final BiFunction<Long, String, Long> timestampMilliTZToLong = (mss, zone) -> DateTimeUtils.fromJavaTimestamp(Timestamp.valueOf(LocalDateTime.from(Instant.ofEpochMilli(mss).atZone(ZoneId.of(zone)))));
    public static final BiFunction<Long, String, Long> timestampNanoTZToLong = (ns, zone) -> DateTimeUtils.fromJavaTimestamp(Timestamp.valueOf(LocalDateTime.from(Instant.ofEpochMilli(ns / 1000000L).atZone(ZoneId.of(zone)))));
    public static final BiFunction<Long, String, Long> timestampMicroTZToLong = (ms, zone) -> DateTimeUtils.fromJavaTimestamp(Timestamp.valueOf(LocalDateTime.from(Instant.ofEpochMilli(ms / 1000L).atZone(ZoneId.of(zone))).plusNanos(ms % 1_000 * 1000L)));
    public static final Function<Timestamp, Long> timestampToLong = DateTimeUtils::fromJavaTimestamp;
    static final Function<Integer, UTF8String> timeSecToString = (ts) -> {
        int hours = ts / 3600;
        int minutes = (ts - hours * 3600) / 60;
        int seconds = (ts - hours * 3600 - minutes * 60);
        return TypeConversionHelper.stringToUtf8String.apply(String.format("%02d:%02d:%02d", hours, minutes, seconds));
    };
    static final Function<LocalDateTime, UTF8String> timeMilliToString = (ldt) -> TypeConversionHelper.stringToUtf8String.apply(String.format("%02d:%02d:%02d.%03d", ldt.getHour(), ldt.getMinute(), ldt.getSecond(), ldt.getNano() / 1000000L));
    static final Function<Long, UTF8String> timeMicroToString = (ms) -> {
        int totalSeconds = (int) (ms / 1000000L);
        long microSeconds = ms - totalSeconds * 1000000L;
        int hours = totalSeconds / 3600;
        int minutes = (totalSeconds - hours * 3600) / 60;
        int seconds = (totalSeconds - hours * 3600 - minutes * 60);
        return TypeConversionHelper.stringToUtf8String.apply(String.format("%02d:%02d:%02d.%06d", hours, minutes, seconds, microSeconds));
    };
    static final Function<Long, UTF8String> timeNanoToString = (ns) -> {
        int totalSeconds = (int) (ns / 1000000000L);
        long nanoSeconds = totalSeconds * 1000000000L;
        int hours = totalSeconds / 3600;
        int minutes = (totalSeconds - hours * 3600) / 60;
        int seconds = (totalSeconds - hours * 3600 - minutes * 60);
        return TypeConversionHelper.stringToUtf8String.apply(String.format("%02d:%02d:%02d.%09d", hours, minutes, seconds, nanoSeconds));
    };
    public static final Function<Duration, Long> durationToLong = IntervalUtils::durationToMicros;
    public static final Function<Period, Integer> periodToInt = IntervalUtils::periodToMonths;
    public static final Function<PeriodDuration, InternalRow> translatePeriodDuration =
            (pd) -> InternalRow.fromSeq(JavaConverters.asScalaBuffer(List.<Object>of(
                    TypeConversionHelper.periodToInt.apply(pd.getPeriod()),
                    TypeConversionHelper.durationToLong.apply(pd.getDuration()))));

    @SuppressWarnings("UstableApiUsage")
    static final ArrowConversion.ConvertFrom<Map<String, ?>, StructVector, FieldType.StructType, InternalRow> translateStruct = (m, sv, t) -> {
        Map<String, FieldType> mt = t.getChildrenType();
        BiFunction<Map.Entry<String, ?>, Long, Object> setV = (e, i) -> {
            String k = e.getKey();
            Object v = e.getValue();
            return (v != null && mt.containsKey(k)) ? TypeConversionHelper.translate.apply(mt.get(k), v, sv.getVectorById(i.intValue())) : null;
        };
        List<Object> nm = new java.util.ArrayList<>();
        Streams.mapWithIndex(m.entrySet().stream(), setV::apply).forEach(nm::add);
        return InternalRow.fromSeq(JavaConverters.asScalaBuffer(Arrays.asList(nm.toArray())).toSeq());
    };
    static final ArrowConversion.ConvertFrom<Map<String, ?>, StructVector, FieldType.StructType, ArrayBasedMapData> structToMap = (m, sv, t) -> {
        Map<String, FieldType> mt = t.getChildrenType();
        if (mt.size() == 1 && mt.containsKey("map") && mt.get("map").getTypeID() == FieldType.IDs.LIST && sv.getVectorById(0) instanceof ListVector lv) {
            FieldType.ListType lt = (FieldType.ListType) mt.get("map");
            if (lt.getChildType().getTypeID() == FieldType.IDs.STRUCT && lv.getDataVector() instanceof StructVector dv) {
                FieldType.StructType st = (FieldType.StructType) lt.getChildType();
                String[] childKeys = st.getChildrenType().keySet().toArray(new String[0]);
                if (childKeys.length == 2 && childKeys[0].equals(KEY_FIELD_NAME)
                        && childKeys[1].equals(VALUE_FIELD_NAME)) {
                    List<Map.Entry<String, ?>> kvs = new java.util.ArrayList<>(m.entrySet());
                    if (kvs.size() == 1 && kvs.getFirst().getKey().equals("map") && kvs.getFirst().getValue() instanceof List<?> list) {
                        FieldType kt = st.getChildrenType().get(KEY_FIELD_NAME);
                        FieldType vt = st.getChildrenType().get(VALUE_FIELD_NAME);
                        List<Object> keys = new java.util.ArrayList<>();
                        List<Object> values = new java.util.ArrayList<>();
                        list.forEach(e -> {
                            Map<?, ?> mes = (Map<?, ?>) e;
                            mes.forEach((key, value) -> {
                                keys.add(TypeConversionHelper.translate.apply(kt, key, dv.getVectorById(0)));
                                values.add(TypeConversionHelper.translate.apply(vt, value, dv.getVectorById(1)));
                            });
                        });
                        return new ArrayBasedMapData(ArrayData.toArrayData(keys.toArray()), ArrayData.toArrayData(values.toArray()));
                    }
                }
            }
        }
        return null;
    };

    /**
     * Casts a FieldVector to a specific vector subtype.
     * @param fv the field vector to cast
     * @param <V> target vector type
     * @return cast vector
     * @throws RuntimeException if cast fails
     */
    @SuppressWarnings("unchecked")
    static <V extends org.apache.arrow.vector.FieldVector> V cast(org.apache.arrow.vector.FieldVector fv) {
        try {
            return (V) fv;
        } catch (Exception e) {
            throw new RuntimeException(String.format("ArrowVector casting to [%s] failed.", fv.getClass().getTypeName()), e);
        }
    }


    @SuppressWarnings("unchecked")
    static final ArrowConversion.ConvertFrom<FieldType, Object, ValueVector, Object> translate = (t, o, v) -> switch (t.getTypeID()) {
        case VARCHAR, CHAR -> TypeConversionHelper.stringToUtf8String.apply(o.toString());
        case TIMESTAMP -> translateTimestamp(o, v);
        case TIME -> translateTime(o, v);
        case DATE -> translateDate(o, v);
        case DECIMAL -> TypeConversionHelper.bigDecimalToDecimal.apply((BigDecimal) o);
        case DURATION_DAY_TIME ->
            (v instanceof IntervalDayVector || v instanceof DurationVector) ? TypeConversionHelper.durationToLong.apply((Duration) o) : o;
        case PERIOD_YEAR_MONTH ->
            (v instanceof IntervalYearVector) ? TypeConversionHelper.periodToInt.apply((Period) o) : o;
        case PERIOD_DURATION_MONTH_DAY_TIME ->
            (v instanceof IntervalMonthDayNanoVector) ? TypeConversionHelper.translatePeriodDuration.apply((PeriodDuration) o) : o;
        case LIST ->
            (v instanceof ListVector) ? TypeConversionHelper.translateList.apply((List<?>) o, (ListVector) v, (FieldType.ListType) t) : o;
        case MAP ->
            (v instanceof MapVector) ? TypeConversionHelper.translateMap.apply((List<?>) o, (MapVector) v, (FieldType.MapType) t) : o;
        case STRUCT ->
            (v instanceof StructVector) ? TypeConversionHelper.mapElseStruct.apply((Map<String, ?>) o, (StructVector) v, (FieldType.StructType) t) : o;
        case NULL -> null;
        default -> o;
    };

    /**
     * Converts a timestamp according to its Arrow vector type.
     *
     * @param value timestamp value
     * @param vector destination vector
     * @return converted timestamp value
     */
    private static Object translateTimestamp(Object value, ValueVector vector) {
        if (vector instanceof TimeStampMilliVector) {
            return timestampToLong.apply((Timestamp) value);
        }
        if (vector instanceof TimeStampMicroTZVector timestampVector) {
            return timestampMicroTZToLong.apply((Long) value, timestampVector.getTimeZone());
        }
        if (vector instanceof TimeStampSecTZVector timestampVector) {
            return timestampSecTZToLong.apply((Long) value, timestampVector.getTimeZone());
        }
        if (vector instanceof TimeStampMilliTZVector timestampVector) {
            return timestampMilliTZToLong.apply((Long) value, timestampVector.getTimeZone());
        }
        if (vector instanceof TimeStampNanoTZVector timestampVector) {
            return timestampNanoTZToLong.apply((Long) value, timestampVector.getTimeZone());
        }
        if (vector instanceof TimeStampMicroVector
                || vector instanceof TimeStampSecVector
                || vector instanceof TimeStampNanoVector) {
            return localDateTimeToLong.apply((LocalDateTime) value);
        }
        return value;
    }

    /**
     * Converts a time according to its Arrow vector type.
     *
     * @param value time value
     * @param vector destination vector
     * @return converted time value
     */
    private static Object translateTime(Object value, ValueVector vector) {
        if (vector instanceof TimeSecVector) {
            return timeSecToString.apply((Integer) value);
        }
        if (vector instanceof TimeMilliVector) {
            return timeMilliToString.apply((LocalDateTime) value);
        }
        if (vector instanceof TimeMicroVector) {
            return timeMicroToString.apply((Long) value);
        }
        if (vector instanceof TimeNanoVector) {
            return timeNanoToString.apply((Long) value);
        }
        return value;
    }

    /**
     * Converts a date according to its Arrow vector type.
     *
     * @param value date value
     * @param vector destination vector
     * @return converted date value
     */
    private static Object translateDate(Object value, ValueVector vector) {
        if (vector instanceof DateDayVector) {
            return dateDayToInt.applyAsInt((Integer) value);
        }
        if (vector instanceof DateMilliVector) {
            return localDateTimeToInt.apply((LocalDateTime) value);
        }
        return value;
    }


    static final ArrowConversion.ConvertFrom<Map<String, ?>, StructVector, FieldType.StructType, Object> mapElseStruct = (m, sv, t) -> TypeConversionHelper.o1ElseO2.apply(TypeConversionHelper.structToMap.apply(m, sv, t), TypeConversionHelper.translateStruct.apply(m, sv, t));
    static final ArrowConversion.ConvertFrom<List<?>, MapVector, FieldType.MapType, ArrayBasedMapData> translateMap = (l, mv, mt) -> {
        List<Object> keys = new java.util.ArrayList<>();
        List<Object> values = new java.util.ArrayList<>();
        Function<ValueVector[], Boolean> probe = (vs) -> (vs.length == 2
                && vs[0].getField().getName().equalsIgnoreCase(KEY_FIELD_NAME)
                && vs[1].getField().getName().equalsIgnoreCase(VALUE_FIELD_NAME));
        Consumer<ValueVector[]> populate = (vs) -> {
            if (probe.apply(vs)) {
                l.forEach(e -> {
                    JsonStringHashMap<?, ?> entry = (JsonStringHashMap<?, ?>) e;
                    keys.add(translate.apply(mt.getKeyType(), entry.get(KEY_FIELD_NAME), vs[0]));
                    values.add(translate.apply(mt.getValueType(), entry.get(VALUE_FIELD_NAME), vs[1]));
                });
            }
        };
        ValueVector[] fields = mv.getChildrenFromFields().toArray(new ValueVector[0]);
        populate.accept((fields.length == 1 && fields[0] instanceof StructVector) ? ((StructVector) fields[0]).getChildrenFromFields().toArray(new ValueVector[0]) : fields);
        return new ArrayBasedMapData(ArrayData.toArrayData(keys.toArray()), ArrayData.toArrayData(values.toArray()));
    };
    static final ArrowConversion.ConvertFrom<List<?>, ListVector, FieldType.ListType, ArrayData> translateList = (l, v, t) -> ArrayData.toArrayData(l.stream().map(e -> TypeConversionHelper.translate.apply(t.getChildType(), e, v.getDataVector())).toArray());

}
