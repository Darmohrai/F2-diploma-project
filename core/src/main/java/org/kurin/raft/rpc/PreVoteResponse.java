package org.kurin.raft.rpc;

/**
 * @param term the current term of the responding node
 */
public record PreVoteResponse(
        long term,
        boolean voteGranted
) {
}
