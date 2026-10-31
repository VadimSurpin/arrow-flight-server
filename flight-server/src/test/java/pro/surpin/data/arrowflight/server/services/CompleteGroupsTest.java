package pro.surpin.data.arrowflight.server.services;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import org.junit.jupiter.api.Test;

class CompleteGroupsTest {
    @Test
    void takesTheHintOffAndNamesTheColumn() {
        CompleteGroups.Request r = CompleteGroups.parse(CompleteGroups.hint("l_orderkey", "select a\nfrom t group by a"));
        assertEquals("l_orderkey", r.column());
        assertEquals("select a\nfrom t group by a", r.sql());
    }

    @Test
    void queriesWithoutAHintAreUntouched() {
        CompleteGroups.Request r = CompleteGroups.parse("select 1 /*+ complete_groups(x) */");
        assertNull(r.column());
        assertEquals("select 1 /*+ complete_groups(x) */", r.sql());
    }

    @Test
    void rejectsColumnsThatAreNotPlainNames() {
        assertNull(CompleteGroups.parse("/*+ complete_groups(a; drop) */ select 1").column());
    }
}
