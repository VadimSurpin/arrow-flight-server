package pro.surpin.data.arrowflight.server.services;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Finds out whether the files of a table can be handed out so that no value of a column occurs in two groups.
 *
 * <p>Each file contributes the smallest and largest value of the column. Files whose ranges overlap (or touch) may hold
 * the same value, so they form one inseparable group; a file that overlaps nothing is a group of its own. Two different
 * groups never share a value, so grouping rows by this column inside one group of files gives complete groups.
 */
public final class KeySeparation {

    /**
     * Smallest and largest value of a column in one file.
     *
     * @param min smallest value
     * @param max largest value
     */
    public record Range(double min, double max) { }

    private KeySeparation() {
    }

    /**
     * Number of groups the files fall into for a column, or zero when some file has no usable range.
     *
     * @param perFile ranges of every column of every file
     * @param column column name
     * @return group count; one means nothing can be separated
     */
    public static int groupCount(Map<String, Map<String, Range>> perFile, String column) {
        Map<String, Range> ranges = new HashMap<>();
        for (Map.Entry<String, Map<String, Range>> file : perFile.entrySet()) {
            Range range = file.getValue().get(column);
            if (range == null) {
                return 0;
            }
            ranges.put(file.getKey(), range);
        }
        return ranges.isEmpty() ? 0 : groups(ranges).size();
    }

    /**
     * Splits files into groups that cannot share a value of the column.
     *
     * <p>Doubles round monotonically, so two ranges that overlap exactly always overlap as doubles; a rounding error can
     * only merge groups that were separate, which is the safe direction.
     *
     * @param ranges value range per file
     * @return groups of files, ordered by their smallest value; a single group means nothing can be separated
     */
    public static List<List<String>> groups(Map<String, Range> ranges) {
        List<Map.Entry<String, Range>> ordered = new ArrayList<>(ranges.entrySet());
        ordered.sort(Comparator.<Map.Entry<String, Range>>comparingDouble(e -> e.getValue().min())
                .thenComparing(Map.Entry::getKey));
        List<List<String>> groups = new ArrayList<>();
        List<String> current = null;
        double currentMax = Double.NEGATIVE_INFINITY;
        for (Map.Entry<String, Range> entry : ordered) {
            Range range = entry.getValue();
            if (current == null || range.min() > currentMax) {
                current = new ArrayList<>();
                groups.add(current);
                currentMax = range.max();
            } else {
                currentMax = Math.max(currentMax, range.max());
            }
            current.add(entry.getKey());
        }
        return groups;
    }

    /**
     * Value range of every top-level numeric column of a file that has complete statistics and no nulls. Columns with
     * missing statistics, nulls or non-numeric values are left out, because their ranges cannot prove anything.
     *
     * @param footer Parquet footer
     * @return range per column name
     */
    public static Map<String, Range> ranges(
            org.apache.parquet.hadoop.metadata.ParquetMetadata footer) {
        Map<String, double[]> bounds = new HashMap<>();
        Set<String> unusable = new HashSet<>();
        for (org.apache.parquet.hadoop.metadata.BlockMetaData block : footer.getBlocks()) {
            for (org.apache.parquet.hadoop.metadata.ColumnChunkMetaData chunk : block.getColumns()) {
                if (chunk.getPath().size() != 1) {
                    continue;
                }
                String name = chunk.getPath().toDotString();
                org.apache.parquet.column.statistics.Statistics<?> statistics = chunk.getStatistics();
                if (statistics == null || statistics.isEmpty() || !statistics.isNumNullsSet()
                        || statistics.getNumNulls() != 0 || !statistics.hasNonNullValue()) {
                    unusable.add(name);
                    continue;
                }
                Double low = TableStatisticsService.bound(statistics.genericGetMin(), chunk.getPrimitiveType());
                Double high = TableStatisticsService.bound(statistics.genericGetMax(), chunk.getPrimitiveType());
                if (low == null || high == null) {
                    unusable.add(name);
                    continue;
                }
                bounds.merge(name, new double[] {low, high},
                        (a, b) -> new double[] {Math.min(a[0], b[0]), Math.max(a[1], b[1])});
            }
        }
        Map<String, Range> result = new HashMap<>();
        bounds.forEach((name, range) -> {
            if (!unusable.contains(name)) {
                result.put(name, new Range(range[0], range[1]));
            }
        });
        return result;
    }
}
