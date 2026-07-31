package pro.surpin.data.arrowflight.client.model;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class TypeConversionTest {

    @Test
    void timeSecToStringZeroTime() {
        assertEquals("00:00:00", TypeConversionHelper.timeSecToString.apply(0).toString());
    }

    @Test
    void timeSecToStringMidday() {
        assertEquals("12:00:00", TypeConversionHelper.timeSecToString.apply(43200).toString());
    }

    @Test
    void timeSecToStringEndOfDay() {
        assertEquals("23:59:59", TypeConversionHelper.timeSecToString.apply(86399).toString());
    }

    @Test
    void timeMicroToStringZeroTime() {
        assertEquals("00:00:00.000000", TypeConversionHelper.timeMicroToString.apply(0L).toString());
    }

    @Test
    void timeMicroToStringWithMicros() {
        assertEquals("00:00:01.500000", TypeConversionHelper.timeMicroToString.apply(1_500_000L).toString());
    }

    @Test
    void timeNanoToStringZeroTime() {
        assertEquals("00:00:00.000000000", TypeConversionHelper.timeNanoToString.apply(0L).toString());
    }

    @Test
    void microsToNanosConvertsViaMillis() {
        // microsToNanos: micros → DateTimeUtils.microsToMillis() → ×1000
        assertEquals(1000L, TypeConversionHelper.microsToNanos.applyAsLong(1000L));
    }

    @Test
    void microsToSecsDivides() {
        assertEquals(1L, TypeConversionHelper.microsToSecs.applyAsLong(1000000L));
    }

    @Test
    void o1ElseO2ReturnsFirstWhenNotNull() {
        assertEquals("first", TypeConversionHelper.o1ElseO2.apply("first", "second"));
    }

    @Test
    void o1ElseO2ReturnsSecondWhenFirstIsNull() {
        assertEquals("second", TypeConversionHelper.o1ElseO2.apply(null, "second"));
    }
}
