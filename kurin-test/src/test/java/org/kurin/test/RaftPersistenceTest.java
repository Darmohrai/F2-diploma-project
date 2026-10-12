package org.kurin.test;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.kurin.api.CommandOptions;
import org.kurin.raft.RaftConfig;
import org.kurin.test.KurinTestCluster.Node;

import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

class RaftPersistenceTest {
    private static final Duration ELECTION = Duration.ofSeconds(10);

    private KurinTestCluster cluster;

    @AfterEach
    void stopCluster() {
        if (cluster != null) {
            cluster.close();
        }
    }

    // Spec test 7
    @Test
    void restartedEmptyNodeCatchesUpThroughSnapshot() throws Exception {
        cluster = KurinTestCluster.builder().nodes(3)
                .services(() -> List.of(new CounterService()))
                .configure((index, builder) -> {
                    builder.raftConfig().setSnapshotThresholdEntries(100);
                    // Small chunks, so that the snapshot really travels in several pieces
                    builder.raftConfig().setMaxAppendBytes(2048);
                })
                .build()
                .start();
        Node leader = cluster.awaitLeader(ELECTION);
        Node lagging = cluster.followers().get(0);

        leader.kurin().submitCommand(new CounterService.Add(1), CommandOptions.withRequestId("first")).get(5, TimeUnit.SECONDS);
        cluster.kill(lagging);
        for (int i = 0; i < 350; i++) {
            leader.kurin().submitCommand(new CounterService.Add(1)).get(5, TimeUnit.SECONDS);
        }
        long leaderApplied = leader.status().lastApplied();

        // MEMORY storage: the node comes back with nothing, and the entries it needs are compacted away on the leader
        cluster.restart(lagging);
        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
            assertThat(lagging.status().lastApplied()).isGreaterThanOrEqualTo(leaderApplied);
            assertThat(lagging.service(CounterService.class).value()).isEqualTo(351);
            assertThat(lagging.service(CounterService.class).appliedCommands()).isEqualTo(351);
        });

        // The deduplication table came with the snapshot: an old request is recognised by every node, also after failover
        cluster.kill(leader);
        Node newLeader = cluster.awaitLeader(ELECTION);
        int repeated = newLeader.kurin().<Integer>submitCommand(new CounterService.Add(1),
                CommandOptions.withRequestId("first")).get(5, TimeUnit.SECONDS);
        assertThat(repeated).isEqualTo(1);
        assertThat(lagging.service(CounterService.class).value()).isEqualTo(351);
    }

    // Spec test 8
    @Test
    void fileStorageKeepsStateAcrossFullClusterRestart(@TempDir Path dataDir) throws Exception {
        cluster = KurinTestCluster.builder().nodes(3)
                .storage(RaftConfig.StorageType.FILE)
                .dataDir(dataDir)
                .services(() -> List.of(new CounterService()))
                .configure((index, builder) -> builder.raftConfig().setSnapshotThresholdEntries(40))
                .build()
                .start();
        Node leader = cluster.awaitLeader(ELECTION);
        // 100 commands with a threshold of 40: the state is split between a snapshot and the log tail
        for (int i = 0; i < 100; i++) {
            leader.kurin().submitCommand(new CounterService.Add(2)).get(5, TimeUnit.SECONDS);
        }
        leader.kurin().submitCommand(new CounterService.Add(5), CommandOptions.withRequestId("bonus")).get(5, TimeUnit.SECONDS);
        long applied = leader.status().lastApplied();
        long term = leader.status().term();
        cluster.awaitApplied(applied, Duration.ofSeconds(5));

        cluster.nodes().forEach(cluster::kill);
        cluster.nodes().forEach(cluster::restart);
        Node newLeader = cluster.awaitLeader(ELECTION);

        assertThat(newLeader.status().term()).isGreaterThan(term);
        await().atMost(Duration.ofSeconds(10)).untilAsserted(() ->
                assertThat(cluster.nodes()).allSatisfy(node -> {
                    assertThat(node.status().lastApplied()).isGreaterThanOrEqualTo(applied);
                    assertThat(node.service(CounterService.class).value()).isEqualTo(205);
                }));

        // Requests applied before the restart are still deduplicated, and the cluster accepts new work
        int repeated = newLeader.kurin().<Integer>submitCommand(new CounterService.Add(5),
                CommandOptions.withRequestId("bonus")).get(5, TimeUnit.SECONDS);
        int next = newLeader.kurin().<Integer>submitCommand(new CounterService.Add(1)).get(5, TimeUnit.SECONDS);
        assertThat(repeated).isEqualTo(205);
        assertThat(next).isEqualTo(206);
    }

    @Test
    void singleRestartedNodeRejoinsFromItsOwnDisk(@TempDir Path dataDir) throws Exception {
        cluster = KurinTestCluster.builder().nodes(3)
                .storage(RaftConfig.StorageType.FILE)
                .dataDir(dataDir)
                .services(() -> List.of(new CounterService()))
                .build()
                .start();
        Node leader = cluster.awaitLeader(ELECTION);
        Node follower = cluster.followers().get(0);
        for (int i = 0; i < 20; i++) {
            leader.kurin().submitCommand(new CounterService.Add(1)).get(5, TimeUnit.SECONDS);
        }
        cluster.awaitInSync(Duration.ofSeconds(5));
        long termBefore = follower.status().term();

        cluster.restart(follower);

        await().atMost(Duration.ofSeconds(10)).untilAsserted(() ->
                assertThat(follower.service(CounterService.class).value()).isEqualTo(20));
        // The persisted term survived: the node did not rejoin from term 0
        assertThat(follower.status().term()).isGreaterThanOrEqualTo(termBefore);
    }
}
