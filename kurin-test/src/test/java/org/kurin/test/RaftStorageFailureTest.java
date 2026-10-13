package org.kurin.test;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.kurin.api.KurinObserver;
import org.kurin.api.ReadConsistency;
import org.kurin.api.exception.KurinStorageException;
import org.kurin.raft.RaftConfig;
import org.kurin.raft.RaftRole;
import org.kurin.test.KurinTestCluster.Node;

import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

class RaftStorageFailureTest {
    private static final Duration ELECTION = Duration.ofSeconds(10);
    private static final Duration SOON = Duration.ofSeconds(10);

    private KurinTestCluster cluster;
    private final Map<Integer, List<String>> storageEvents = new ConcurrentHashMap<>();

    @AfterEach
    void stopCluster() {
        if (cluster != null) {
            cluster.close();
        }
    }

    private KurinTestCluster.Builder threeNodes() {
        return KurinTestCluster.builder().nodes(3)
                .services(() -> List.of(new CounterService()))
                .configure((index, builder) -> {
                    // Retries often enough for a test, with the same doubling as in production
                    builder.raftConfig().setStorageRetryMinMs(20);
                    builder.raftConfig().setStorageRetryMaxMs(160);
                    List<String> events = storageEvents.computeIfAbsent(index, key -> new CopyOnWriteArrayList<>());
                    builder.addObserver(new KurinObserver() {
                        @Override
                        public void onStorageFailure(Throwable cause) {
                            events.add("failed");
                        }

                        @Override
                        public void onStorageRecovered() {
                            events.add("recovered");
                        }
                    });
                });
    }

    private static int add(Node node, int delta) throws Exception {
        return node.kurin().<Integer>submitCommand(new CounterService.Add(delta)).get(5, TimeUnit.SECONDS);
    }

    @Test
    void leaderWithFailedStorageYieldsAndReturnsAfterRepair() throws Exception {
        cluster = threeNodes().build().start();
        Node oldLeader = cluster.awaitLeader(ELECTION);
        assertThat(add(oldLeader, 1)).isEqualTo(1);
        cluster.awaitInSync(SOON);
        long termBefore = oldLeader.status().term();

        cluster.breakStorage(oldLeader);
        // The command that hits the broken disk is refused with the reason, not left to time out
        assertThatThrownBy(() -> add(oldLeader, 10))
                .isInstanceOf(ExecutionException.class)
                .hasCauseInstanceOf(KurinStorageException.class);

        await().atMost(SOON).untilAsserted(() -> {
            assertThat(oldLeader.status().storageFailed()).isTrue();
            assertThat(oldLeader.status().storageFailure()).contains("Injected storage failure");
            assertThat(oldLeader.status().role()).isEqualTo(RaftRole.FOLLOWER);
            assertThat(oldLeader.kurin().leadership()).isEmpty();
        });

        // The two healthy nodes elect a leader among themselves and keep committing
        Node newLeader = cluster.awaitLeader(ELECTION);
        assertThat(newLeader).isNotSameAs(oldLeader);
        assertThat(newLeader.status().term()).isGreaterThan(termBefore);
        for (int i = 0; i < 20; i++) {
            add(newLeader, 1);
        }
        assertThat(newLeader.service(CounterService.class).value()).isEqualTo(21);

        // The failed node serves neither commands nor reads, not even local ones, and says why
        assertThatThrownBy(() -> add(oldLeader, 1)).hasCauseInstanceOf(KurinStorageException.class);
        assertThatThrownBy(() -> oldLeader.kurin().submitQuery(new CounterService.GetValue(), ReadConsistency.LOCAL)
                .get(5, TimeUnit.SECONDS)).hasCauseInstanceOf(KurinStorageException.class);
        // It took no part in the election: its term is the one it had when the disk broke
        assertThat(oldLeader.status().term()).isEqualTo(termBefore);
        assertThat(storageEvents.get(oldLeader.index())).containsExactly("failed");

        cluster.repairStorage(oldLeader);
        await().atMost(SOON).untilAsserted(() -> assertThat(oldLeader.status().storageFailed()).isFalse());
        cluster.awaitInSync(SOON);

        assertThat(cluster.nodes()).allSatisfy(node -> assertThat(node.service(CounterService.class).value()).isEqualTo(21));
        assertThat(oldLeader.status().leaderId()).isEqualTo(newLeader.nodeId());
        assertThat(storageEvents.get(oldLeader.index())).containsExactly("failed", "recovered");
        // Back in full: it forwards commands again
        assertThat(add(oldLeader, 1)).isEqualTo(22);
    }

    @Test
    void failedFollowerDoesNotHoldBackTheMajority() throws Exception {
        cluster = threeNodes().build().start();
        Node leader = cluster.awaitLeader(ELECTION);
        Node broken = cluster.followers().get(0);
        Node healthy = cluster.followers().get(1);
        add(leader, 1);
        cluster.awaitInSync(SOON);
        long term = leader.status().term();

        cluster.breakStorage(broken);
        for (int i = 0; i < 30; i++) {
            add(leader, 1);
        }

        await().atMost(SOON).untilAsserted(() -> assertThat(broken.status().storageFailed()).isTrue());
        assertThat(healthy.service(CounterService.class).value()).isEqualTo(31);
        // Nothing the broken node could not store was applied on it
        assertThat(broken.service(CounterService.class).value()).isEqualTo(1);
        // The leader was not disturbed
        assertThat(leader.kurin().leadership()).isPresent();
        assertThat(leader.status().term()).isEqualTo(term);

        cluster.repairStorage(broken);
        await().atMost(SOON).untilAsserted(() -> {
            assertThat(broken.status().storageFailed()).isFalse();
            assertThat(broken.service(CounterService.class).value()).isEqualTo(31);
        });
        assertThat(leader.status().term()).isEqualTo(term);
    }

    @Test
    void failedSnapshotStoreTakesTheNodeOutUntilItWorks() throws Exception {
        cluster = threeNodes()
                .configure((index, builder) -> {
                    builder.raftConfig().setSnapshotThresholdEntries(20);
                    builder.raftConfig().setStorageRetryMinMs(20);
                    builder.raftConfig().setStorageRetryMaxMs(160);
                })
                .build().start();
        Node leader = cluster.awaitLeader(ELECTION);
        Node broken = cluster.followers().get(0);

        // The log still works: the node keeps up until it has to take a snapshot
        cluster.storageFaults(broken).setSnapshotsBroken(true);
        for (int i = 0; i < 50; i++) {
            add(leader, 1);
        }
        await().atMost(SOON).untilAsserted(() -> assertThat(broken.status().storageFailed()).isTrue());
        assertThat(cluster.storageFaults(broken).refusedWrites()).isPositive();

        cluster.repairStorage(broken);
        await().atMost(SOON).untilAsserted(() -> {
            assertThat(broken.status().storageFailed()).isFalse();
            assertThat(broken.service(CounterService.class).value()).isEqualTo(50);
        });
    }

    @Test
    void fileLogIsRewrittenAfterRepairAndSurvivesARestart(@TempDir Path dataDir) throws Exception {
        cluster = threeNodes().storage(RaftConfig.StorageType.FILE).dataDir(dataDir).build().start();
        Node leader = cluster.awaitLeader(ELECTION);
        Node broken = cluster.followers().get(0);
        for (int i = 0; i < 10; i++) {
            add(leader, 1);
        }
        cluster.awaitInSync(SOON);

        cluster.breakStorage(broken);
        for (int i = 0; i < 10; i++) {
            add(leader, 1);
        }
        await().atMost(SOON).untilAsserted(() -> assertThat(broken.status().storageFailed()).isTrue());

        cluster.repairStorage(broken);
        await().atMost(SOON).untilAsserted(() -> assertThat(broken.status().storageFailed()).isFalse());
        cluster.awaitInSync(SOON);
        long applied = leader.status().lastApplied();

        // What the node holds after the repair is really on its disk: it comes back from there alone
        cluster.nodes().forEach(cluster::kill);
        cluster.restart(broken);
        Node other = cluster.nodes().stream().filter(node -> node != broken).findFirst().orElseThrow();
        cluster.restart(other);
        cluster.awaitLeader(ELECTION);
        await().atMost(SOON).untilAsserted(() -> {
            assertThat(broken.status().lastApplied()).isGreaterThanOrEqualTo(applied);
            assertThat(broken.service(CounterService.class).value()).isEqualTo(20);
        });
    }
}
