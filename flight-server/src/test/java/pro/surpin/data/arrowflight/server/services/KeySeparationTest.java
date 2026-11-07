package pro.surpin.data.arrowflight.server.services;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import pro.surpin.data.arrowflight.server.services.KeySeparation.Range;

class KeySeparationTest {

    @Test
    void disjointFilesAreSeparateGroups() {
        Map<String, Range> ranges = new LinkedHashMap<>();
        ranges.put("b", new Range(11, 20));
        ranges.put("a", new Range(1, 10));
        ranges.put("c", new Range(21, 30));
        assertEquals(List.of(List.of("a"), List.of("b"), List.of("c")), KeySeparation.groups(ranges));
    }

    @Test
    void overlappingAndTouchingFilesShareAGroup() {
        Map<String, Range> ranges = new LinkedHashMap<>();
        ranges.put("a", new Range(1, 10));
        ranges.put("b", new Range(10, 20));
        ranges.put("c", new Range(15, 18));
        ranges.put("d", new Range(40, 50));
        assertEquals(List.of(List.of("a", "b", "c"), List.of("d")), KeySeparation.groups(ranges));
    }

    @Test
    void aWideFileSwallowsTheOnesInsideIt() {
        Map<String, Range> ranges = new LinkedHashMap<>();
        ranges.put("wide", new Range(0, 100));
        ranges.put("x", new Range(10, 20));
        ranges.put("y", new Range(30, 40));
        assertEquals(List.of(List.of("wide", "x", "y")), KeySeparation.groups(ranges));
    }

    @Test
    void identicalRangesCannotBeSeparated() {
        Map<String, Range> ranges = new LinkedHashMap<>();
        for (String f : List.of("a", "b", "c")) {
            ranges.put(f, new Range(1, 14_999_999));
        }
        assertEquals(1, KeySeparation.groups(ranges).size());
    }
}
