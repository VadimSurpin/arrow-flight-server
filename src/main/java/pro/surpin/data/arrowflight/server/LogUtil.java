package pro.surpin.data.arrowflight.server;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

/**
 * Utility for formatting log context fields consistently.
 * Every log entry for a query flow includes qid, node, thread, endpoint/ticket,
 * and elapsed time. This class provides cached node identity and elapsed formatters.
 */
public final class LogUtil {

    private static final String NODE;

    /**
     * Thread-local qid propagated across async execution boundaries.
     */
    private static final ThreadLocal<String> CURRENT_QID = new ThreadLocal<>();

    static {
        String n = System.getenv("HOSTNAME");
        if (n == null || n.isBlank()) {
            try {
                n = java.net.InetAddress.getLocalHost().getHostName();
            } catch (Exception e) {
                n = "unknown";
            }
        }
        NODE = n;
    }

    private LogUtil() {
    }

    /**
     * Returns the node name (hostname) for log correlation.
     *
     * @return node name
     */
    public static String node() {
        return NODE;
    }

    /**
     * Sets the current thread's qid.
     *
     * @param qid query identifier, null to clear
     */
    public static void setQid(String qid) {
        if (qid != null) {
            CURRENT_QID.set(qid);
        } else {
            CURRENT_QID.remove();
        }
    }

    /**
     * Returns the current thread's qid, or null.
     *
     * @return qid or null
     */
    public static String qid() {
        return CURRENT_QID.get();
    }

    /**
     * Formats elapsed nanos as a human-readable duration.
     *
     * @param startNanos System.nanoTime() at operation start
     * @return formatted string like "1.234ms" or "0.567s"
     */
    public static String elapsedNanos(long startNanos) {
        long nanos = System.nanoTime() - startNanos;
        if (nanos < 1_000) {
            return nanos + "ns";
        }
        if (nanos < 1_000_000) {
            return String.format("%.1fµs", nanos / 1000.0);
        }
        if (nanos < 1_000_000_000) {
            return String.format("%.2fms", nanos / 1_000_000.0);
        }
        return String.format("%.3fs", nanos / 1_000_000_000.0);
    }

    /**
     * Formats elapsed wall-clock time in millis.
     *
     * @param startMillis System.currentTimeMillis() at operation start
     * @return formatted string like "1234ms" or "1.234s"
     */
    public static String elapsedMs(long startMillis) {
        long elapsed = System.currentTimeMillis() - startMillis;
        if (elapsed < 10_000) {
            return elapsed + "ms";
        }
        return String.format("%.3fs", elapsed / 1000.0);
    }

    /**
     * Builds a logging prefix with qid, node, thread.
     *
     * @param qid query identifier, may be null
     * @return formatted prefix
     */
    public static String prefix(String qid) {
        return "node=" + NODE
                + " thread=" + Thread.currentThread().getName()
                + (qid != null ? " qid=" + qid : "");
    }

    /**
     * Wraps a Runnable to capture and restore the current qid.
     *
     * @param task  original task
     * @return task with qid context propagated
     */
    public static Runnable withQid(Runnable task) {
        String captured = CURRENT_QID.get();
        if (captured == null) {
            return task;
        }
        return () -> {
            String previous = CURRENT_QID.get();
            CURRENT_QID.set(captured);
            try {
                task.run();
            } finally {
                if (previous != null) {
                    CURRENT_QID.set(previous);
                } else {
                    CURRENT_QID.remove();
                }
            }
        };
    }

    // ── Timing (DEBUG-level structured timing events) ─────────────────────

    private static final Logger TIMING_LOG = LoggerFactory.getLogger("Timing");

    /**
     * Returns a nanosecond timestamp if timing is enabled, or -1 if disabled.
     * Pass the returned value to {@link #logTiming(long, String, String)}.
     *
     * @return System.nanoTime() when DEBUG is enabled, -1 otherwise
     */
    public static long mark() {
        return TIMING_LOG.isDebugEnabled() ? System.nanoTime() : -1L;
    }

    /**
     * Logs a timing event at DEBUG level. No-op when timing is disabled.
     *
     * @param mark  value from {@link #mark()}
     * @param event event name
     */
    public static void logTiming(long mark, String event) {
        logTiming(mark, event, "");
    }

    /**
     * Logs a timing event with extra key=value context at DEBUG level.
     * No-op when timing is disabled.
     *
     * @param mark  value from {@link #mark()}
     * @param event event name
     * @param extra extra context (e.g. "files=12 engine=DuckDB"), empty string to omit
     */
    public static void logTiming(long mark, String event, String extra) {
        if (mark < 0) {
            return;
        }
        long elapsedMicros = TimeUnit.NANOSECONDS.toMicros(System.nanoTime() - mark);
        TIMING_LOG.debug("qid={} node={} TIMING thread={} durationUs={} tag={}{}",
                qid(), NODE, Thread.currentThread().getName(), elapsedMicros, event,
                extra.isEmpty() ? "" : " " + extra);
    }

}
