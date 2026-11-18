package pro.surpin.data.arrowflight.server;

import java.util.Map;
import java.util.function.Predicate;

/** Removes expired cache entries and caps retained entries. */
public final class CacheLimits {
    private CacheLimits() { }

    /**
     * Removes expired entries and evicts arbitrary cache entries above the limit.
     *
     * @param entries cache entries
     * @param expired identifies entries whose TTL has passed
     * @param maximum maximum retained entries
     * @param <K> key type
     * @param <V> value type
     */
    public static <K, V> void prune(Map<K, V> entries, Predicate<V> expired, int maximum) {
        if (entries.size() <= maximum) {
            return;
        }
        entries.entrySet().removeIf(entry -> expired.test(entry.getValue()));
        int excess = entries.size() - maximum;
        if (excess <= 0) {
            return;
        }
        for (K key : entries.keySet()) {
            if (entries.remove(key) != null && --excess <= 0) {
                break;
            }
        }
    }
}
