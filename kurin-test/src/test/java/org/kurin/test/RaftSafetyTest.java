package org.kurin.test;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.kurin.api.LeadershipToken;
import org.kurin.api.ReadConsistency;
import org.kurin.api.exception.KurinNotLeaderException;
import org.kurin.raft.RaftStatus;
import org.kurin.test.KurinTestCluster.Node;

import java.time.Duration;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

class RaftSafetyTest {
    private static final Duration ELECTION = Duration.ofSeconds(10);
    // Timings of KurinTestCluster
    private static final long HEARTBEAT_MS = 30;
    private static final long ELECTION_TIMEOUT_MIN_MS = 300;
    private static final long ELECTION_TIMEOUT_MAX_MS = 600;
    // Allowance for thread scheduling and for the polling interval of the assertions
    private static final long SCHEDULING_SLACK_MS = 250;

    private KurinTestCluster cluster;
    private final ScheduledExecutorService sampler = Executors.newSingleThreadScheduledExecutor();

    @AfterEach
    void stopCluster() {
        sampler.shutdownNow();
        if (cluster != null) {
            cluster.close();
        }
    }

    private KurinTestCluster threeNodes() {
        cluster = KurinTestCluster.builder().nodes(3).services(() -> List.of(new CounterService())).build();
        return cluster.start();
    }

    // Spec test 2
    @Test
    void isolatedLeaderStepsDownAndLeasesNeverOverlap() throws Exception {
        threeNodes();
        Node oldLeader = cluster.awaitLeader(ELECTION);
        List<Node> majority = cluster.followers();

        AtomicInteger maxSimultaneousLeases = new AtomicInteger();
        sampler.scheduleAtFixedRate(() -> {
            long now = System.nanoTime();
            int leases = 0;
            for (Node node : cluster.nodes()) {
                RaftStatus status = node.status();
                if (status.isLeader() && status.leaseValidUntilNanos() != 0 && status.leaseValidUntilNanos() - now > 0) {
                    leases++;
                }
            }
            maxSimultaneousLeases.accumulateAndGet(leases, Math::max);
        }, 0, 5, TimeUnit.MILLISECONDS);

        long partitionedAt = System.nanoTime();
        cluster.partition(Set.of(oldLeader), Set.copyOf(majority));

        await().atMost(Duration.ofSeconds(5)).pollInterval(Duration.ofMillis(5)).until(() -> !oldLeader.status().isLeader());
        long stepDownMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - partitionedAt);
        assertThat(stepDownMs).isLessThanOrEqualTo(ELECTION_TIMEOUT_MIN_MS + HEARTBEAT_MS + SCHEDULING_SLACK_MS);
        assertThat(oldLeader.kurin().leadership()).isEmpty();

        Node newLeader = cluster.awaitLeader(ELECTION);
        assertThat(majority).contains(newLeader);
        assertThat(newLeader.status().term()).isGreaterThan(oldLeader.status().term());

        // The majority keeps working, the isolated node cannot commit anything
        int value = newLeader.kurin().<Integer>submitCommand(new CounterService.Add(1)).get(5, TimeUnit.SECONDS);
        assertThat(value).isEqualTo(1);
        assertThat(oldLeader.service(CounterService.class).value()).isZero();

        cluster.heal();
        await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> {
            assertThat(oldLeader.status().leaderId()).isEqualTo(newLeader.nodeId());
            assertThat(oldLeader.service(CounterService.class).value()).isEqualTo(1);
        });

        assertThat(maxSimultaneousLeases.get()).isEqualTo(1);
    }

    // Spec test 3
    @Test
    void returningPartitionedNodeDoesNotRaiseTheClusterTerm() throws Exception {
        threeNodes();
        Node leader = cluster.awaitLeader(ELECTION);
        Node isolated = cluster.followers().get(0);
        Node other = cluster.followers().get(1);
        long term = leader.status().term();

        cluster.partition(Set.of(isolated), Set.of(leader, other));

        // Several election timeouts pass on the isolated node: without PreVote its term would climb each time
        await().during(Duration.ofMillis(4 * ELECTION_TIMEOUT_MAX_MS)).atMost(Duration.ofSeconds(10))
                .until(() -> isolated.status().term() == term);
        assertThat(isolated.status().term()).isEqualTo(term);

        cluster.heal();
        await().atMost(Duration.ofSeconds(5)).until(() -> leader.nodeId().equals(isolated.status().leaderId()));

        int value = isolated.kurin().<Integer>submitCommand(new CounterService.Add(2)).get(5, TimeUnit.SECONDS);
        assertThat(value).isEqualTo(2);
        assertThat(cluster.leader()).contains(leader);
        assertThat(cluster.nodes()).allSatisfy(node -> assertThat(node.status().term()).isEqualTo(term));
    }

    // Spec test 4
    @Test
    void linearizableReadOnAnyNodeSeesTheWriteThatJustCompleted() throws Exception {
        threeNodes();
        cluster.awaitLeader(ELECTION);
        List<Node> nodes = cluster.nodes();

        for (int i = 1; i <= 30; i++) {
            Node writer = nodes.get(i % nodes.size());
            writer.kurin().submitCommand(new CounterService.Add(1)).get(5, TimeUnit.SECONDS);
            for (Node reader : nodes) {
                int seen = reader.kurin().<Integer>submitQuery(new CounterService.GetValue(), ReadConsistency.LINEARIZABLE)
                        .get(5, TimeUnit.SECONDS);
                assertThat(seen).as("read on %s after write %d on %s", reader, i, writer).isEqualTo(i);
            }
        }
    }

    @Test
    void leaseAndLocalReadsFollowTheirContracts() throws Exception {
        threeNodes();
        Node leader = cluster.awaitLeader(ELECTION);
        Node follower = cluster.followers().get(0);
        leader.kurin().submitCommand(new CounterService.Add(5)).get(5, TimeUnit.SECONDS);

        int leaseRead = follower.kurin().<Integer>submitQuery(new CounterService.GetValue(), ReadConsistency.LEASE).get(5, TimeUnit.SECONDS);
        assertThat(leaseRead).isEqualTo(5);

        // A local read is served by the follower itself and catches up eventually
        await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> assertThat(
                follower.kurin().<Integer>submitQuery(new CounterService.GetValue(), ReadConsistency.LOCAL).get(1, TimeUnit.SECONDS)).isEqualTo(5));

        // Cut off from the majority, the leader loses its lease: LEASE reads are refused, LOCAL reads still answer
        cluster.partition(Set.of(leader), Set.copyOf(cluster.followers()));
        await().atMost(Duration.ofSeconds(5)).until(() -> leader.kurin().leadership().isEmpty());
        assertThatThrownBy(() -> leader.kurin().submitQuery(new CounterService.GetValue(), ReadConsistency.LEASE).get(5, TimeUnit.SECONDS))
                .isInstanceOf(ExecutionException.class)
                .cause()
                .isInstanceOfAny(KurinNotLeaderException.class, org.kurin.api.exception.KurinNoLeaderException.class);
        int staleRead = leader.kurin().<Integer>submitQuery(new CounterService.GetValue(), ReadConsistency.LOCAL).get(5, TimeUnit.SECONDS);
        assertThat(staleRead).isEqualTo(5);
    }

    @Test
    void leadershipTokenIsHeldByTheLeaderOnlyAndGrowsWithEveryNewLeader() {
        threeNodes();
        Node first = cluster.awaitLeader(ELECTION);
        LeadershipToken firstToken = first.kurin().leadership().orElseThrow();

        assertThat(firstToken.nodeId()).isEqualTo(first.nodeId());
        assertThat(firstToken.term()).isEqualTo(first.status().term());
        assertThat(cluster.followers()).allSatisfy(node -> assertThat(node.kurin().leadership()).isEmpty());

        cluster.kill(first);
        Node second = cluster.awaitLeader(ELECTION);
        LeadershipToken secondToken = second.kurin().leadership().orElseThrow();

        assertThat(secondToken.term()).isGreaterThan(firstToken.term());
        assertThat(secondToken.acquiredAtLogIndex()).isGreaterThan(firstToken.acquiredAtLogIndex());
    }
}
