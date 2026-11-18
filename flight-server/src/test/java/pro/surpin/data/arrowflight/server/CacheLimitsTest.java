package pro.surpin.data.arrowflight.server;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

/** Tests bounded cache housekeeping shared by metadata services. */
class CacheLimitsTest {
    /** Verifies expired entries are removed before live entries when capacity is exceeded. */
    @Test
    void evictsExpiredEntriesFirst() {
        Map<String, Integer> entries = new ConcurrentHashMap<>();
        entries.put("expired", -1);
        entries.put("live", 1);
        entries.put("new", 2);

        CacheLimits.prune(entries, value -> value < 0, 2);

        assertEquals(2, entries.size());
        assertFalse(entries.containsKey("expired"));
    }

    /** Verifies live entries are bounded when none have expired. */
    @Test
    void capsLiveEntries() {
        Map<String, Integer> entries = new ConcurrentHashMap<>();
        entries.put("a", 1);
        entries.put("b", 2);
        entries.put("c", 3);

        CacheLimits.prune(entries, value -> false, 2);

        assertEquals(2, entries.size());
    }
}
