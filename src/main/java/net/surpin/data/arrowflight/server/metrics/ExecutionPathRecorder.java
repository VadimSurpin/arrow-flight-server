package net.surpin.data.arrowflight.server.metrics;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import net.surpin.data.arrowflight.server.LogUtil;
import net.surpin.data.arrowflight.server.model.ExecutionPath;
import net.surpin.data.arrowflight.server.model.ExecutionPathTracker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.util.HexFormat;
import java.util.Locale;

/**
 * Emits structured runtime execution-path evidence for benchmark collection.
 */
public final class ExecutionPathRecorder {

    private static final Logger LOGGER = LoggerFactory.getLogger(ExecutionPathRecorder.class);
    private static final ObjectMapper JSON = new ObjectMapper();
    private static final Object FILE_LOCK = new Object();
    private static final String OUTPUT_ENV = "BENCHMARK_EXECUTION_PATH_LOG";

    private ExecutionPathRecorder() {
    }

    /**
     * Emits one execution-path event to logs and the optional JSONL evidence file.
     *
     * @param qid endpoint query identifier
     * @param query logical SQL query
     * @param tracker runtime path tracker
     * @param success whether execution completed successfully
     * @param failureReason failure description or null
     */
    public static void record(String qid, String query, ExecutionPathTracker tracker,
            boolean success, String failureReason) {
        String event = toJson(qid, query, tracker, success, failureReason);
        ExecutionPath path = tracker.path();
        LOGGER.info("qid={} node={} executionPath={} pushdownEvidence={} success={} event={}",
                qid, LogUtil.node(), path.label(), path.isPushdownEvidence(), success, event);

        String output = System.getenv(OUTPUT_ENV);
        if (output == null || output.isBlank()) {
            return;
        }
        try {
            append(Path.of(output), event);
        } catch (IOException e) {
            LOGGER.warn("qid={} node={} Could not append execution-path evidence to {}",
                    qid, LogUtil.node(), output, e);
        }
    }

    /**
     * Serializes one execution-path event as a JSON object.
     *
     * @param qid endpoint query identifier
     * @param query logical SQL query
     * @param tracker runtime path tracker
     * @param success whether execution completed successfully
     * @param failureReason failure description or null
     * @return serialized event
     */
    public static String toJson(String qid, String query, ExecutionPathTracker tracker,
            boolean success, String failureReason) {
        ExecutionPath path = tracker.path();
        ObjectNode event = JSON.createObjectNode();
        event.put("schema_version", "1.0.0");
        event.put("timestamp", Instant.now().toString());
        event.put("qid", qid);
        event.put("node", LogUtil.node());
        event.put("query_digest", queryDigest(query));
        event.put("execution_path", path.label());
        event.put("pushdown_evidence", path.isPushdownEvidence());
        event.put("success", success);
        putNullable(event, "fallback_target",
                tracker.fallbackTarget() == null ? null : tracker.fallbackTarget().label());
        putNullable(event, "reason", tracker.reason());
        putNullable(event, "failure_reason", sanitize(failureReason));
        try {
            return JSON.writeValueAsString(event);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Could not serialize execution-path event", e);
        }
    }

    /**
     * Computes the stable digest of a whitespace-normalized SQL query.
     *
     * @param query SQL query
     * @return lowercase SHA-256 digest
     */
    public static String queryDigest(String query) {
        String normalized = query == null ? "" : query.strip()
                .replaceAll("\\s+", " ")
                .toLowerCase(Locale.ROOT);
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            return HexFormat.of().formatHex(
                    digest.digest(normalized.getBytes(StandardCharsets.UTF_8)));
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 is unavailable", e);
        }
    }

    /**
     * Writes a nullable string field.
     *
     * @param event destination JSON object
     * @param field field name
     * @param value field value
     */
    private static void putNullable(ObjectNode event, String field, String value) {
        if (value == null) {
            event.putNull(field);
        } else {
            event.put(field, value);
        }
    }

    /**
     * Removes line breaks and bounds a failure description.
     *
     * @param value untrusted failure description
     * @return sanitized description or null
     */
    private static String sanitize(String value) {
        if (value == null) {
            return null;
        }
        String sanitized = value.replaceAll("[\\r\\n]+", " ").strip();
        return sanitized.length() <= 1024 ? sanitized : sanitized.substring(0, 1024);
    }

    /**
     * Appends a serialized event atomically within this process.
     *
     * @param output JSONL output path
     * @param event serialized event
     * @throws IOException if the event cannot be appended
     */
    private static void append(Path output, String event) throws IOException {
        synchronized (FILE_LOCK) {
            Path parent = output.getParent();
            if (parent != null) {
                Files.createDirectories(parent);
            }
            Files.writeString(output, event + System.lineSeparator(),
                    StandardCharsets.UTF_8, StandardOpenOption.CREATE,
                    StandardOpenOption.APPEND);
        }
    }
}
