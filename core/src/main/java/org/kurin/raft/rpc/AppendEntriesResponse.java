package org.kurin.raft.rpc;

/**
 * Response to an AppendEntries RPC.
 *
 * @param term          the current term of the responding node
 * @param success       whether the follower's log matched {@code prevLogIndex} and {@code prevLogTerm}
 * @param conflictIndex on rejection: the first index of the conflicting term, or the follower's
 *                      {@code lastLogIndex + 1} if its log is too short; lets the leader skip a whole term at once
 * @param conflictTerm  on rejection: the term of the conflicting entry, 0 if the follower has no entry there
 */
public record AppendEntriesResponse(
        long term,
        boolean success,
        long conflictIndex,
        long conflictTerm
) {
    public static AppendEntriesResponse accepted(long term) {
        return new AppendEntriesResponse(term, true, 0, 0);
    }

    public static AppendEntriesResponse rejected(long term, long conflictIndex, long conflictTerm) {
        return new AppendEntriesResponse(term, false, conflictIndex, conflictTerm);
    }
}
