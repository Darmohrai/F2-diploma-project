package org.kurin.raft.rpc;

/**
 * Response to a RequestVote RPC.
 *
 * @param term        the current term of the responding node
 * @param voteGranted whether the responding node granted its vote to the candidate
 */
public record RequestVoteResponse(
        long term,
        boolean voteGranted
) {
}
