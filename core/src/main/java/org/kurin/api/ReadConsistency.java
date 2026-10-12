package org.kurin.api;

/**
 * How fresh the state seen by a query must be.
 */
public enum ReadConsistency {
    /**
     * The query observes every command acknowledged before it was submitted. The leader confirms that it is
     * still the leader (by a valid lease or a heartbeat round acknowledged by a majority) before answering.
     */
    LINEARIZABLE,
    /**
     * Like {@link #LINEARIZABLE}, but answered only while the leader holds a valid lease, without an extra
     * round trip. Fails with {@link org.kurin.api.exception.KurinNotLeaderException} otherwise. Relies on
     * bounded clock drift between nodes.
     */
    LEASE,
    /**
     * Answered by the node that received the query, leader or not, from whatever it has applied so far.
     * The result may be stale by an unbounded amount, for example on a node cut off from the cluster.
     */
    LOCAL
}
