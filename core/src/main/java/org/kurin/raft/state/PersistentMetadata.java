package org.kurin.raft.state;

import org.kurin.network.model.NodeAddress;

public interface PersistentMetadata {
    long getCurrentTerm();

    void setCurrentTerm(long term);

    NodeAddress getVotedFor();

    void setVotedFor(NodeAddress address);

    void flush();
}
