package org.kurin.raft.state;

import org.kurin.network.model.NodeAddress;

/**
 * Durable {@code currentTerm} and {@code votedFor}.
 */
public interface PersistentMetadata {
    long getCurrentTerm();

    NodeAddress getVotedFor();

    /**
     * Stores both values durably before returning.
     *
     * @throws java.io.UncheckedIOException if the values cannot be persisted; the node must not answer
     *                                      a vote or acknowledge a term it may forget
     */
    void save(long term, NodeAddress votedFor);
}
