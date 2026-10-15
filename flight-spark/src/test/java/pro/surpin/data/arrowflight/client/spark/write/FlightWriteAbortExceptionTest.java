package pro.surpin.data.arrowflight.client.spark.write;

import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.*;

class FlightWriteAbortExceptionTest {

    @Test
    void abortWithoutEpoch() {
        FlightWriteAbortException ex = new FlightWriteAbortException(0, 10L, 42);

        assertTrue(ex instanceof IOException);
        assertTrue(ex.getMessage().contains("42 messages"));
        assertTrue(ex.getMessage().contains("partition (0)"));
        assertTrue(ex.getMessage().contains("task (10)"));
        assertTrue(ex.getMessage().contains("aborted"));
        assertFalse(ex.getMessage().contains("epoch"));
    }

    @Test
    void abortWithEpoch() {
        FlightWriteAbortException ex = new FlightWriteAbortException(3, 7L, "e-1", 99);

        assertTrue(ex.getMessage().contains("99 messages"));
        assertTrue(ex.getMessage().contains("partition (3)"));
        assertTrue(ex.getMessage().contains("task (7)"));
        assertTrue(ex.getMessage().contains("epoch (e-1)"));
        assertTrue(ex.getMessage().contains("aborted"));
    }
}
