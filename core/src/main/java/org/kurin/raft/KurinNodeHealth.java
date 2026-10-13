package org.kurin.raft;

import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Health of a node as a member of its cluster, in a form that depends on no monitoring library. Adapters turn
 * it into the health model of whatever the application uses.
 *
 * @param details facts about the node, in a stable order, with values a JSON writer can handle
 */
public record KurinNodeHealth(Status status, Map<String, Object> details) {

    public enum Status {
        /**
         * The node is the leader with a valid lease, or a follower that heard from its leader within the maximum
         * election timeout.
         */
        UP,
        /**
         * An election is under way or the leader was just lost: requests may or may not succeed.
         */
        UNKNOWN,
        /**
         * The node is not started, or has known no leader for longer than the threshold: commands submitted
         * here cannot succeed.
         */
        OUT_OF_SERVICE,
        /**
         * The node cannot write its durable state and has left the cluster until the storage works again.
         */
        DOWN
    }

    /**
     * @param noLeaderThreshold time without a known leader after which the node counts as out of service
     */
    public static KurinNodeHealth of(KurinNode node, Duration noLeaderThreshold) {
        RaftStatus status = node.status();
        Duration electionTimeoutMax = Duration.ofMillis(node.config().getElectionTimeoutMaxMs());
        Duration leaderless = node.leaderUnknownFor();
        Status result;
        if (!node.isStarted()) {
            result = Status.OUT_OF_SERVICE;
        } else if (status.storageFailed()) {
            // The node is running but takes no part in the cluster and serves no request
            result = Status.DOWN;
        } else if (status.role() == RaftRole.LEADER) {
            // A leader that lost its lease is about to step down
            result = node.leadership().isPresent() ? Status.UP : Status.UNKNOWN;
        } else if (status.leaderId() != null) {
            Duration sinceLeader = node.peerContactAges().get(status.leaderId());
            result = sinceLeader != null && sinceLeader.compareTo(electionTimeoutMax) < 0 ? Status.UP : Status.UNKNOWN;
        } else {
            result = leaderless.compareTo(noLeaderThreshold) > 0 ? Status.OUT_OF_SERVICE : Status.UNKNOWN;
        }
        Map<String, Object> details = describe(node);
        if (status.leaderId() == null) {
            details.put("leaderUnknownForMs", leaderless.toMillis());
        }
        return new KurinNodeHealth(result, details);
    }

    /**
     * @return what the node knows about itself and its peers right now; a new, modifiable map
     */
    public static Map<String, Object> describe(KurinNode node) {
        RaftStatus status = node.status();
        Map<String, Object> details = new LinkedHashMap<>();
        details.put("nodeId", status.nodeId());
        details.put("started", node.isStarted());
        details.put("role", status.role().name());
        details.put("term", status.term());
        details.put("leaderId", status.leaderId());
        details.put("leadershipEstablished", node.leadership().isPresent());
        details.put("commitIndex", status.commitIndex());
        details.put("lastApplied", status.lastApplied());
        details.put("lastLogIndex", status.lastLogIndex());
        details.put("logEntries", node.logEntries());
        if (status.storageFailed()) {
            details.put("storageFailure", status.storageFailure());
        }
        Map<String, Object> peers = new LinkedHashMap<>();
        Map<String, Duration> contacts = node.peerContactAges();
        node.peers().forEach(peer -> {
            Duration age = contacts.get(peer.asString());
            peers.put(peer.asString(), Map.of("lastContactMsAgo", age != null ? age.toMillis() : "unknown"));
        });
        details.put("peers", peers);
        return details;
    }
}
