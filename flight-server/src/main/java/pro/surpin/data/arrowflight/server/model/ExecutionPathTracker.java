package pro.surpin.data.arrowflight.server.model;

import java.util.Objects;

/**
 * Tracks the execution path selected while one query is running.
 */
public final class ExecutionPathTracker {

    private ExecutionPath path = ExecutionPath.UNKNOWN;
    private ExecutionPath fallbackTarget;
    private String reason = "execution-path-not-selected";

    /**
     * Selects a concrete runtime execution path.
     *
     * @param selectedPath selected path
     */
    public void select(ExecutionPath selectedPath) {
        path = Objects.requireNonNull(selectedPath, "selectedPath");
        fallbackTarget = null;
        reason = null;
    }

    /**
     * Records an attempted optimization that fell back to another engine path.
     *
     * @param target fallback target
     * @param fallbackReason reason for fallback
     */
    public void fallbackTo(ExecutionPath target, String fallbackReason) {
        path = ExecutionPath.FALLBACK;
        fallbackTarget = Objects.requireNonNull(target, "target");
        reason = Objects.requireNonNull(fallbackReason, "fallbackReason");
    }

    /**
     * Records that no concrete execution path could be selected.
     *
     * @param unknownReason reason the path is unknown
     */
    public void unknown(String unknownReason) {
        path = ExecutionPath.UNKNOWN;
        fallbackTarget = null;
        reason = Objects.requireNonNull(unknownReason, "unknownReason");
    }

    /**
     * Returns the selected stable path.
     *
     * @return selected path
     */
    public ExecutionPath path() {
        return path;
    }

    /**
     * Returns the concrete fallback target when one was used.
     *
     * @return fallback target or null
     */
    public ExecutionPath fallbackTarget() {
        return fallbackTarget;
    }

    /**
     * Returns the fallback or unknown-path reason.
     *
     * @return reason or null
     */
    public String reason() {
        return reason;
    }
}
