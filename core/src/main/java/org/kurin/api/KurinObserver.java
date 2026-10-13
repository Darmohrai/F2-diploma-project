package org.kurin.api;

/**
 * Receives measurements from the Raft core. The Spring starter turns them into Micrometer meters; without it
 * an application can register its own observer on the node builder.
 * <p>
 * Most callbacks run on the Raft thread. They must be fast and must not block; an exception thrown by an
 * observer is logged and otherwise ignored.
 */
public interface KurinObserver {

    /**
     * @param result {@code started}, {@code won} or {@code lost}
     */
    default void onElection(String result) {
    }

    /**
     * This node learned of a new leader, possibly itself.
     */
    default void onLeaderChange(String leaderId) {
    }

    /**
     * On the leader: an entry it appended was committed.
     *
     * @param latencyNanos time from appending the entry to committing it
     */
    default void onCommit(long latencyNanos) {
    }

    /**
     * An entry was applied to the state machine.
     *
     * @param type simple class name of the command, or {@code NOOP}
     */
    default void onApply(String type, long durationNanos) {
    }

    /**
     * A request to another node completed.
     *
     * @param type simple class name of the request
     */
    default void onRpc(String type, String peer, long latencyNanos, boolean success) {
    }

    /**
     * On the leader: a batch of entries was sent to a follower.
     */
    default void onAppendBatch(int entries) {
    }

    default void onSnapshot(long durationNanos, long bytes) {
    }
}
