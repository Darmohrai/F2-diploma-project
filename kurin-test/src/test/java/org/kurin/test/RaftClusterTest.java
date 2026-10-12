package org.kurin.test;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.kurin.api.CommandOptions;
import org.kurin.api.exception.KurinCommandFailedException;
import org.kurin.api.exception.KurinSerializationException;
import org.kurin.api.exception.KurinTimeoutException;
import org.kurin.test.KurinTestCluster.Node;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

class RaftClusterTest {
    private static final Duration ELECTION = Duration.ofSeconds(10);
    private static final long ELECTION_TIMEOUT_MAX_MS = 600;

    private KurinTestCluster cluster;

    @AfterEach
    void stopCluster() {
        if (cluster != null) {
            cluster.close();
        }
    }

    private KurinTestCluster threeNodes() {
        cluster = KurinTestCluster.builder().nodes(3).services(() -> List.of(new CounterService())).build();
        return cluster.start();
    }

    // Spec test 1
    @Test
    void electsLeaderAndReElectsAfterLeaderIsKilled() {
        threeNodes();
        Node firstLeader = cluster.awaitLeader(ELECTION);
        assertThat(cluster.runningNodes().stream().filter(node -> node.status().isLeader())).hasSize(1);

        long killedAt = System.nanoTime();
        cluster.kill(firstLeader);
        Node secondLeader = cluster.awaitLeader(ELECTION);
        long reElectionMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - killedAt);

        assertThat(secondLeader).isNotSameAs(firstLeader);
        assertThat(secondLeader.status().term()).isGreaterThan(firstLeader.status().term());
        assertThat(reElectionMs).isLessThan(2 * ELECTION_TIMEOUT_MAX_MS);
    }

    // Spec test 5
    @Test
    void voidCommandSubmittedOnFollowerSucceeds() throws Exception {
        threeNodes();
        Node leader = cluster.awaitLeader(ELECTION);
        Node follower = cluster.followers().get(0);

        leader.kurin().submitCommand(new CounterService.Add(5)).get(5, TimeUnit.SECONDS);
        Object result = follower.kurin().submitCommand(new CounterService.Reset()).get(5, TimeUnit.SECONDS);

        assertThat(result).isNull();
        await().atMost(Duration.ofSeconds(5)).untilAsserted(() ->
                assertThat(cluster.nodes()).allSatisfy(node -> {
                    assertThat(node.service(CounterService.class).appliedCommands()).isEqualTo(2);
                    assertThat(node.service(CounterService.class).value()).isZero();
                }));
    }

    // Spec test 5
    @Test
    void handlerExceptionIsTheSameCommandResultOnEveryNode() throws Exception {
        threeNodes();
        cluster.awaitLeader(ELECTION);

        for (Node node : cluster.nodes()) {
            assertThatThrownBy(() -> node.kurin().submitCommand(new CounterService.Fail("boom")).get(5, TimeUnit.SECONDS))
                    .isInstanceOf(ExecutionException.class)
                    .cause()
                    .isInstanceOfSatisfying(KurinCommandFailedException.class, failure -> {
                        assertThat(failure.causeType()).isEqualTo(IllegalStateException.class.getName());
                        assertThat(failure.causeMessage()).isEqualTo("boom");
                    });
        }

        // The log keeps moving after failed commands
        int value = cluster.node(0).kurin().<Integer>submitCommand(new CounterService.Add(7)).get(5, TimeUnit.SECONDS);
        assertThat(value).isEqualTo(7);

        await().atMost(Duration.ofSeconds(5)).untilAsserted(() ->
                assertThat(cluster.nodes()).allSatisfy(node -> {
                    CounterService service = node.service(CounterService.class);
                    assertThat(service.failedCommands()).isEqualTo(3);
                    assertThat(service.appliedCommands()).isEqualTo(4);
                    assertThat(service.value()).isEqualTo(7);
                    assertThat(node.status().lastApplied()).isEqualTo(cluster.node(0).status().lastApplied());
                }));
    }

    // Spec test 6
    @Test
    void retryWithSameRequestIdAfterTimeoutIsAppliedOnce() throws Exception {
        threeNodes();
        Node leader = cluster.awaitLeader(ELECTION);
        List<Node> followers = cluster.followers();

        // Replication is slower than the caller is willing to wait: the command commits after the timeout
        followers.forEach(follower -> cluster.delay(leader, follower, Duration.ofMillis(250)));
        CommandOptions options = CommandOptions.withRequestId("payment-42").timeout(Duration.ofMillis(60));
        assertThatThrownBy(() -> leader.kurin().submitCommand(new CounterService.Add(10), options).get(5, TimeUnit.SECONDS))
                .isInstanceOf(ExecutionException.class)
                .hasCauseInstanceOf(KurinTimeoutException.class);

        cluster.heal();
        int first = leader.kurin().<Integer>submitCommand(new CounterService.Add(10),
                CommandOptions.withRequestId("payment-42")).get(5, TimeUnit.SECONDS);
        int second = followers.get(0).kurin().<Integer>submitCommand(new CounterService.Add(10),
                CommandOptions.withRequestId("payment-42")).get(5, TimeUnit.SECONDS);

        assertThat(first).isEqualTo(10);
        assertThat(second).isEqualTo(10);
        await().atMost(Duration.ofSeconds(5)).untilAsserted(() ->
                assertThat(cluster.nodes()).allSatisfy(node -> {
                    assertThat(node.service(CounterService.class).value()).isEqualTo(10);
                    assertThat(node.service(CounterService.class).appliedCommands()).isEqualTo(1);
                }));
    }

    // Spec test 10
    @Test
    void oversizedPayloadFailsWithSerializationExceptionAndConnectionSurvives() throws Exception {
        cluster = KurinTestCluster.builder().nodes(3)
                .services(() -> List.of(new CounterService()))
                .configure((index, builder) -> builder.networkConfig().setMaxPayloadSize(64 * 1024))
                .build()
                .start();
        Node leader = cluster.awaitLeader(ELECTION);
        Node follower = cluster.followers().get(0);
        byte[] oversized = new byte[128 * 1024];

        // On the follower the forwarded request itself exceeds the frame limit
        assertThatThrownBy(() -> follower.kurin().submitCommand(new CounterService.Store(oversized)).get(5, TimeUnit.SECONDS))
                .isInstanceOf(ExecutionException.class)
                .hasCauseInstanceOf(KurinSerializationException.class);
        // On the leader the entry would not fit into a replication batch
        assertThatThrownBy(() -> leader.kurin().submitCommand(new CounterService.Store(oversized)).get(5, TimeUnit.SECONDS))
                .isInstanceOf(ExecutionException.class)
                .hasCauseInstanceOf(KurinSerializationException.class);

        long termBefore = leader.status().term();
        int stored = follower.kurin().<Integer>submitCommand(new CounterService.Store(new byte[1024])).get(5, TimeUnit.SECONDS);
        int value = follower.kurin().<Integer>submitCommand(new CounterService.Add(3)).get(5, TimeUnit.SECONDS);

        assertThat(stored).isEqualTo(1024);
        assertThat(value).isEqualTo(3);
        assertThat(cluster.leader()).contains(leader);
        assertThat(leader.status().term()).isEqualTo(termBefore);
    }

    @Test
    void queryReturnsStateFromAnyNode() throws Exception {
        threeNodes();
        Node leader = cluster.awaitLeader(ELECTION);
        leader.kurin().submitCommand(new CounterService.Add(4)).get(5, TimeUnit.SECONDS);

        for (Node node : cluster.nodes()) {
            int value = node.kurin().<Integer>submitQuery(new CounterService.GetValue()).get(5, TimeUnit.SECONDS);
            assertThat(value).isEqualTo(4);
        }
    }
}
