package org.kurin.api;

/**
 * Proof that a node was the leader with a valid lease at the moment the token was handed out.
 *
 * @param term               the leader term: strictly greater for every later leader, so it can be passed to
 *                           external systems as a fencing token
 * @param nodeId             id of the leader node
 * @param acquiredAtLogIndex index of the entry whose commit established this leadership
 */
public record LeadershipToken(long term, String nodeId, long acquiredAtLogIndex) {
}
