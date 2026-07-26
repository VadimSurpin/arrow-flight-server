package net.surpin.data.arrowflight.server.metrics;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import net.surpin.data.arrowflight.server.model.ExecutionPath;
import net.surpin.data.arrowflight.server.model.ExecutionPathTracker;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests machine-readable runtime execution-path evidence.
 */
@Tag("unit")
class ExecutionPathRecorderTest {

    private static final ObjectMapper JSON = new ObjectMapper();

    /**
     * Verifies every concrete server path is serialized with a stable label.
     *
     * @throws Exception if JSON parsing fails
     */
    @Test
    void serializesFooterScanAggregationAndJoinPaths() throws Exception {
        List<ExecutionPath> paths = List.of(
                ExecutionPath.FOOTER_COUNT,
                ExecutionPath.FOOTER_STATS,
                ExecutionPath.DUCKDB_SCAN,
                ExecutionPath.DUCKDB_AGGREGATION,
                ExecutionPath.DUCKDB_JOIN);

        for (ExecutionPath path : paths) {
            ExecutionPathTracker tracker = new ExecutionPathTracker();
            tracker.select(path);

            JsonNode event = JSON.readTree(ExecutionPathRecorder.toJson(
                    "qid", "SELECT  *\nFROM tpch.lineitem", tracker, true, null));

            assertEquals(path.label(), event.path("execution_path").asText());
            assertTrue(event.path("pushdown_evidence").asBoolean());
            assertTrue(event.path("success").asBoolean());
        }
    }

    /**
     * Verifies fallback evidence names its target without claiming pushdown.
     *
     * @throws Exception if JSON parsing fails
     */
    @Test
    void serializesFallbackAsNonPublishableEvidence() throws Exception {
        ExecutionPathTracker tracker = new ExecutionPathTracker();
        tracker.fallbackTo(
                ExecutionPath.DUCKDB_AGGREGATION, "missing-parquet-footer-statistics");

        JsonNode event = JSON.readTree(ExecutionPathRecorder.toJson(
                "qid", "select min(value) from sample", tracker, true, null));

        assertEquals("fallback", event.path("execution_path").asText());
        assertEquals("duckdb-aggregation", event.path("fallback_target").asText());
        assertEquals("missing-parquet-footer-statistics", event.path("reason").asText());
        assertFalse(event.path("pushdown_evidence").asBoolean());
    }

    /**
     * Verifies failed unknown paths retain a bounded failure reason.
     *
     * @throws Exception if JSON parsing fails
     */
    @Test
    void serializesUnknownFailureExplicitly() throws Exception {
        ExecutionPathTracker tracker = new ExecutionPathTracker();
        tracker.unknown("no-parquet-files");

        JsonNode event = JSON.readTree(ExecutionPathRecorder.toJson(
                "qid", "select * from missing", tracker, false, "failure\nmessage"));

        assertEquals("unknown", event.path("execution_path").asText());
        assertFalse(event.path("pushdown_evidence").asBoolean());
        assertFalse(event.path("success").asBoolean());
        assertEquals("failure message", event.path("failure_reason").asText());
    }

    /**
     * Verifies logically equivalent whitespace produces the same query digest.
     */
    @Test
    void normalizesWhitespaceBeforeDigestingQueries() {
        String first = ExecutionPathRecorder.queryDigest("SELECT *\nFROM T");
        String second = ExecutionPathRecorder.queryDigest(" select   * from t ");

        assertEquals(first, second);
        assertNotEquals(first, ExecutionPathRecorder.queryDigest("select id from t"));
    }
}
