package org.kurin.raft.rpc;

/**
 * Response to an AppendEntries RPC.
 *
 * @param term    the current term of the responding node, allowing the leader to update its term if necessary
 * @param success whether the follower's log contains an entry matching {@code prevLogIndex} and {@code prevLogTerm}
 */
public record AppendEntriesResponse(
        long term,
        boolean success
) {
}
