package org.kurin.raft.state;

import org.kurin.network.model.NodeAddress;

public class InMemoryPersistentMetadata implements PersistentMetadata {
    private long currentTerm;
    private NodeAddress votedFor;

    @Override
    public long getCurrentTerm() {
        return currentTerm;
    }

    @Override
    public NodeAddress getVotedFor() {
        return votedFor;
    }

    @Override
    public void save(long term, NodeAddress votedFor) {
        this.currentTerm = term;
        this.votedFor = votedFor;
    }
}
