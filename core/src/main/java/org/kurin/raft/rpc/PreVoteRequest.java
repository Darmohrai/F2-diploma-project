package org.kurin.raft.rpc;

import org.kurin.network.model.NodeAddress;

/**
 * Asks whether the receiver would vote for the sender in an election for {@code term}, without anyone
 * changing its term (Raft thesis, §9.6). A node cut off from the cluster keeps failing this probe instead
 * of inflating its term and deposing a healthy leader when it reconnects.
 *
 * @param term the term the sender would campaign in, its current term plus one
 */
public record PreVoteRequest(
        long term,
        NodeAddress candidateId,
        long lastLogIndex,
        long lastLogTerm
) implements RaftRpc {
}
