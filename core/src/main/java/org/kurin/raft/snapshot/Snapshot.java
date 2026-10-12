package org.kurin.raft.snapshot;

import org.kurin.raft.state.CommandOutcome;

import java.util.LinkedHashMap;

/**
 * The state of a node after applying the log up to {@code lastIncludedIndex}.
 *
 * @param data       the aggregated state of the state machine services
 * @param dedupTable results of already applied requests by request id, in the order they were applied.
 *                   It travels with the snapshot so that a node restored from it still recognises retries.
 */
public record Snapshot(
        long lastIncludedIndex,
        long lastIncludedTerm,
        byte[] data,
        LinkedHashMap<String, CommandOutcome> dedupTable
) {
}
