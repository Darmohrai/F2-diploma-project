package org.kurin.raft.rpc;

/**
 * @param term       the current term of the responding node
 * @param nextOffset the offset the follower expects next: the bytes it holds so far. After the last chunk it
 *                   equals the size of the snapshot. If the follower already has this snapshot or a newer
 *                   state, {@code installed} is true and the offset is meaningless.
 * @param installed  true once the follower's state covers {@code lastIncludedIndex}
 */
public record InstallSnapshotResponse(
        long term,
        int nextOffset,
        boolean installed
) {
}
