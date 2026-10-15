package pro.surpin.data.arrowflight.client;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests elapsed time formatting used in client logs.
 */
@Tag("unit")
class ElapsedTimeTest {
    /**
     * Verifies that long durations use seconds with three decimal places.
     */
    @Test
    void formatsSeconds() {
        String value = ElapsedTime.format(System.nanoTime() - TimeUnit.SECONDS.toNanos(2));
        assertTrue(value.matches("[0-9]+\\.[0-9]{3}s"));
    }
}
