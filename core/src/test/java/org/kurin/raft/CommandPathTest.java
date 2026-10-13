package org.kurin.raft;

import org.junit.jupiter.api.Test;
import org.kurin.network.client.ConnectionManager.Reachability;
import org.kurin.network.model.NodeAddress;

import java.util.function.Function;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * What a node says about its way to the leader, for every state the node and its connection can be in.
 */
class CommandPathTest {
    private static final String SELF = "10.0.0.1:9000";
    private static final String LEADER = "10.0.0.2:9000";

    private static RaftStatus status(RaftRole role, String leaderId, String storageFailure) {
        return new RaftStatus(role, 3, leaderId, 10, 10, 10, 0, SELF, storageFailure);
    }

    private static Function<NodeAddress, Reachability> connection(Reachability toLeader) {
        return peer -> {
            assertThat(peer).isEqualTo(NodeAddress.parse(LEADER));
            return toLeader;
        };
    }

    private static final Function<NodeAddress, Reachability> NOT_ASKED = peer -> {
        throw new AssertionError("The connection to " + peer + " does not matter here");
    };

    @Test
    void followerDependsOnItsOwnConnectionToTheLeader() {
        RaftStatus follower = status(RaftRole.FOLLOWER, LEADER, null);

        assertThat(CommandPath.of(true, follower, false, connection(Reachability.CONNECTED))).isEqualTo(CommandPath.UP);
        assertThat(CommandPath.of(true, follower, false, connection(Reachability.CONNECTING))).isEqualTo(CommandPath.CONNECTING);
        // Nothing is open and nothing is wrong: the first request opens the connection
        assertThat(CommandPath.of(true, follower, false, connection(Reachability.NOT_CONNECTED))).isEqualTo(CommandPath.CONNECTING);
        // The leader is heard from, but the last attempt to connect to it failed
        assertThat(CommandPath.of(true, follower, false, connection(Reachability.PAUSED))).isEqualTo(CommandPath.DOWN);
    }

    @Test
    void leaderIsItsOwnPathOnceItsLeadershipIsEstablished() {
        RaftStatus leader = status(RaftRole.LEADER, SELF, null);

        assertThat(CommandPath.of(true, leader, true, NOT_ASKED)).isEqualTo(CommandPath.UP);
        // Elected, the first entry of the term not committed yet
        assertThat(CommandPath.of(true, leader, false, NOT_ASKED)).isEqualTo(CommandPath.CONNECTING);
    }

    @Test
    void withoutALeaderOrAWorkingNodeThereIsNoPath() {
        assertThat(CommandPath.of(true, status(RaftRole.FOLLOWER, null, null), false, NOT_ASKED)).isEqualTo(CommandPath.DOWN);
        assertThat(CommandPath.of(true, status(RaftRole.CANDIDATE, null, null), false, NOT_ASKED)).isEqualTo(CommandPath.DOWN);
        assertThat(CommandPath.of(false, status(RaftRole.FOLLOWER, LEADER, null), false, NOT_ASKED)).isEqualTo(CommandPath.DOWN);
        assertThat(CommandPath.of(true, status(RaftRole.FOLLOWER, LEADER, "disk full"), false, NOT_ASKED)).isEqualTo(CommandPath.DOWN);
    }
}
