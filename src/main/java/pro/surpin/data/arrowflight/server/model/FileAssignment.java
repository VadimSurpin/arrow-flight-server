package pro.surpin.data.arrowflight.server.model;

import java.util.Set;

/**
 * Assignment of a file with its size and the set of hosts that hold it.
 */
public record FileAssignment(long size, Set<String> hosts) {}
