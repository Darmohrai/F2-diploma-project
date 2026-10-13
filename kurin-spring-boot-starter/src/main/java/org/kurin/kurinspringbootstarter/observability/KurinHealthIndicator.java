package org.kurin.kurinspringbootstarter.observability;

import org.kurin.raft.KurinNode;
import org.kurin.raft.RaftRole;
import org.kurin.raft.RaftStatus;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.boot.actuate.health.Status;

import java.time.Duration;

/**
 * Health of this instance as a member of the cluster.
 * <ul>
 *   <li>{@code UP}: it is the leader with a valid lease, or a follower that heard from its leader within the
 *       maximum election timeout;</li>
 *   <li>{@code OUT_OF_SERVICE}: it has known no leader for longer than the configured threshold, so commands
 *       submitted here cannot succeed;</li>
 *   <li>{@code UNKNOWN} in between: an election is under way or the leader was just lost;</li>
 *   <li>{@code DOWN}: it cannot write its durable state and has left the cluster until the storage works again.</li>
 * </ul>
 */
public class KurinHealthIndicator implements HealthIndicator {
    private final KurinNode node;
    private final Duration noLeaderThreshold;

    public KurinHealthIndicator(KurinNode node, Duration noLeaderThreshold) {
        this.node = node;
        this.noLeaderThreshold = noLeaderThreshold;
    }

    @Override
    public Health health() {
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

        Health.Builder health = Health.status(result).withDetails(KurinEndpoint.describeNode(node));
        if (status.leaderId() == null) {
            health.withDetail("leaderUnknownForMs", leaderless.toMillis());
        }
        return health.build();
    }
}
