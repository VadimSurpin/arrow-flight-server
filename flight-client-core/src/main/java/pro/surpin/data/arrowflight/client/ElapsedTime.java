package pro.surpin.data.arrowflight.client;

import java.util.Locale;

/**
 * Formats client operation durations for logging.
 */
public final class ElapsedTime {
    private ElapsedTime() {
    }

    /**
     * Formats time elapsed since a nanosecond timestamp.
     *
     * @param startNanos value returned by System.nanoTime()
     * @return human-readable elapsed time
     */
    public static String format(long startNanos) {
        long nanos = System.nanoTime() - startNanos;
        if (nanos < 1_000) {
            return nanos + "ns";
        }
        if (nanos < 1_000_000) {
            return String.format(Locale.ROOT, "%.1fµs", nanos / 1_000.0);
        }
        if (nanos < 1_000_000_000) {
            return String.format(Locale.ROOT, "%.2fms", nanos / 1_000_000.0);
        }
        return String.format(Locale.ROOT, "%.3fs", nanos / 1_000_000_000.0);
    }
}
