package org.kurin.raft.rpc;

import org.kurin.network.model.NodeAddress;

/**
 * Request sent by a candidate to request a vote during an election.
 *
 * @param term         the candidate's current term
 * @param candidateId  the network address identifying the candidate
 * @param lastLogIndex the index of the candidate's last log entry
 * @param lastLogTerm  the term of the candidate's last log entry
 */
public record RequestVoteRequest(
        long term,
        NodeAddress candidateId,
        long lastLogIndex,
        long lastLogTerm
) implements RaftRpc {
}
