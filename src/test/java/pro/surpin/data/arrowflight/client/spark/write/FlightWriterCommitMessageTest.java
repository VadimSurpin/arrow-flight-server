package pro.surpin.data.arrowflight.client.spark.write;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class FlightWriterCommitMessageTest {

    @Test
    void commitWithoutEpoch() {
        FlightWriterCommitMessage msg = new FlightWriterCommitMessage(0, 42L, 1000);

        String m = msg.getMessage();
        assertTrue(m.contains("1000 messages"));
        assertTrue(m.contains("partition (0)"));
        assertTrue(m.contains("task (42)"));
        assertFalse(m.contains("epoch"));
    }

    @Test
    void commitWithEpoch() {
        FlightWriterCommitMessage msg = new FlightWriterCommitMessage(1, 99L, "epoch-7", 500);

        String m = msg.getMessage();
        assertTrue(m.contains("500 messages"));
        assertTrue(m.contains("epoch (epoch-7)"));
    }

    @Test
    void copyConstructorForStreamingRetry() {
        FlightWriterCommitMessage original = new FlightWriterCommitMessage(2, 88L, 200);
        FlightWriterCommitMessage retry = new FlightWriterCommitMessage(original, 5L);

        String m = retry.getMessage();
        assertTrue(m.contains("200 messages"));
        assertTrue(m.contains("partition (2)"));
        assertTrue(m.contains("task (88)"));
        assertTrue(m.contains("epoch (5)"));
    }

}
