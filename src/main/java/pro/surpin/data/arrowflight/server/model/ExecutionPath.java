package pro.surpin.data.arrowflight.server.model;

/**
 * Identifies the runtime-selected execution path for a Flight query.
 */
public enum ExecutionPath {
    FOOTER_COUNT("footer-count", true),
    FOOTER_STATS("footer-stats", true),
    DUCKDB_SCAN("duckdb-scan", true),
    DUCKDB_AGGREGATION("duckdb-aggregation", true),
    DUCKDB_JOIN("duckdb-join", true),
    DISTRIBUTED("distributed", true),
    MIXED("mixed", true),
    FALLBACK("fallback", false),
    UNKNOWN("unknown", false);

    private final String label;
    private final boolean pushdownEvidence;

    ExecutionPath(String label, boolean pushdownEvidence) {
        this.label = label;
        this.pushdownEvidence = pushdownEvidence;
    }

    /**
     * Returns the stable machine-readable path label.
     *
     * @return path label
     */
    public String label() {
        return label;
    }

    /**
     * Reports whether this path can support a pushdown claim.
     *
     * @return true when the path is publishable pushdown evidence
     */
    public boolean isPushdownEvidence() {
        return pushdownEvidence;
    }
}
