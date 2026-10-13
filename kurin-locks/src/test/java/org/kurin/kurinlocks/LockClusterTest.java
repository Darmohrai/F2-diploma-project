package org.kurin.kurinlocks;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.kurin.api.ReadConsistency;
import org.kurin.api.exception.KurinNoLeaderException;
import org.kurin.api.exception.KurinTimeoutException;
import org.kurin.kurinlocks.api.KurinLockManager;
import org.kurin.kurinlocks.api.LockHandle;
import org.kurin.kurinlocks.api.LockOptions;
import org.kurin.kurinlocks.command.LockCommands.CountLocks;
import org.kurin.kurinlocks.command.LockCommands.GetLock;
import org.kurin.kurinlocks.command.LockCommands.LockRecord;
import org.kurin.kurinlocks.command.LockCommands.PurgeExpiredLocks;
import org.kurin.kurinlocks.exception.KurinLockAcquisitionException;
import org.kurin.kurinlocks.job.JobStateMachine;
import org.kurin.kurinlocks.state.LockStateMachine;
import org.kurin.test.KurinTestCluster;
import org.kurin.test.KurinTestCluster.Node;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

class LockClusterTest {
    private static final Duration ELECTION = Duration.ofSeconds(10);

    private KurinTestCluster cluster;
    private final Map<Node, KurinLockManager> managers = new HashMap<>();
    private ExecutorService workers;

    @AfterEach
    void stop() {
        if (workers != null) {
            workers.shutdownNow();
        }
        managers.values().forEach(KurinLockManager::close);
        if (cluster != null) {
            cluster.close();
        }
    }

    private Node startCluster(Duration commandTimeout) {
        return startCluster(commandTimeout, false);
    }

    /**
     * @param patientLeader the leader keeps its role through seconds without an answer. For tests that slow its
     *                      heartbeats down: with the usual timings it steps down after 300 ms of silence.
     */
    private Node startCluster(Duration commandTimeout, boolean patientLeader) {
        cluster = KurinTestCluster.builder().nodes(3)
                .services(() -> List.of(new LockStateMachine(), new JobStateMachine()))
                .configure((index, builder) -> {
                    if (patientLeader) {
                        builder.raftConfig().setElectionTimeoutMinMs(3000);
                        builder.raftConfig().setElectionTimeoutMaxMs(4000);
                    }
                })
                .build()
                .start();
        Node leader = cluster.awaitLeader(patientLeader ? Duration.ofSeconds(30) : ELECTION);
        for (Node node : cluster.nodes()) {
            managers.put(node, new KurinLockManager(node.kurin(), node.service(LockStateMachine.class), commandTimeout));
        }
        return leader;
    }

    private LockRecord localRecord(Node node, String key) throws Exception {
        return node.kurin().<LockRecord>submitQuery(new GetLock(key), ReadConsistency.LOCAL).get(2, TimeUnit.SECONDS);
    }

    // Spec test 11
    @Test
    void criticalSectionsNeverOverlapAndFencingTokensGrow() throws Exception {
        startCluster(Duration.ofSeconds(5));
        int threadsPerNode = 8;
        int roundsPerThread = 4;
        workers = Executors.newFixedThreadPool(cluster.nodes().size() * threadsPerNode);

        AtomicInteger insideCriticalSection = new AtomicInteger();
        AtomicInteger overlaps = new AtomicInteger();
        List<Long> tokensInOrder = Collections.synchronizedList(new ArrayList<>());
        int[] unsafeCounter = {0};
        CountDownLatch start = new CountDownLatch(1);

        List<Future<?>> results = new ArrayList<>();
        for (Node node : cluster.nodes()) {
            KurinLockManager manager = managers.get(node);
            for (int thread = 0; thread < threadsPerNode; thread++) {
                results.add(workers.submit(() -> {
                    start.await();
                    for (int round = 0; round < roundsPerThread; round++) {
                        try (LockHandle lock = manager.lock("counter", Duration.ofSeconds(30), Duration.ofSeconds(60))) {
                            if (insideCriticalSection.incrementAndGet() != 1) {
                                overlaps.incrementAndGet();
                            }
                            tokensInOrder.add(lock.fencingToken());
                            unsafeCounter[0]++;
                            insideCriticalSection.decrementAndGet();
                        }
                    }
                    return null;
                }));
            }
        }
        start.countDown();
        for (Future<?> result : results) {
            result.get(120, TimeUnit.SECONDS);
        }

        int expected = cluster.nodes().size() * threadsPerNode * roundsPerThread;
        assertThat(overlaps.get()).isZero();
        assertThat(unsafeCounter[0]).isEqualTo(expected);
        assertThat(tokensInOrder).hasSize(expected).isSorted().doesNotHaveDuplicates();
    }

    // Spec test 12
    @Test
    void expiryIsDecidedIdenticallyOnEveryNode() throws Exception {
        Node leader = startCluster(Duration.ofSeconds(5));
        Node first = cluster.node(0);
        Node second = cluster.node(1);

        LockHandle abandoned = managers.get(first).tryLock("report", Duration.ofMillis(300)).orElseThrow();
        managers.get(first).tryLock("purge-1", Duration.ofMillis(300)).orElseThrow();
        managers.get(first).tryLock("purge-2", Duration.ofMillis(300)).orElseThrow();
        cluster.awaitInSync(Duration.ofSeconds(5));

        // Every node recorded the same owner, token and expiry
        LockRecord recorded = localRecord(leader, "report");
        assertThat(recorded.fencingToken()).isEqualTo(abandoned.fencingToken());
        for (Node node : cluster.nodes()) {
            assertThat(localRecord(node, "report")).isEqualTo(recorded);
        }
        // While the lock lives, nobody else gets it
        assertThat(managers.get(second).tryLock("report", Duration.ofSeconds(5))).isEmpty();

        await().atMost(Duration.ofSeconds(5)).until(() -> System.currentTimeMillis() > recorded.expiresAtMs() + 50);
        assertThat(abandoned.isHeld()).isFalse();

        // An expired record is overwritten by the next acquisition, with a larger token
        LockHandle successor = managers.get(second).tryLock("report", Duration.ofSeconds(30)).orElseThrow();
        assertThat(successor.fencingToken()).isGreaterThan(abandoned.fencingToken());
        assertThat(abandoned.renew(Duration.ofSeconds(5))).isFalse();

        // Purge removes exactly the expired records, and all nodes end up with the same table
        int purged = leader.kurin().<Integer>submitCommand(new PurgeExpiredLocks()).get(5, TimeUnit.SECONDS);
        assertThat(purged).isEqualTo(2);
        cluster.awaitInSync(Duration.ofSeconds(5));
        for (Node node : cluster.nodes()) {
            assertThat(node.kurin().<Integer>submitQuery(new CountLocks(), ReadConsistency.LOCAL).get(2, TimeUnit.SECONDS)).isEqualTo(1);
            assertThat(localRecord(node, "purge-1")).isNull();
            assertThat(localRecord(node, "report").fencingToken()).isEqualTo(successor.fencingToken());
        }
    }

    // Spec test 13
    @Test
    void acquisitionWithUnknownOutcomeLeavesNoLockBehind() throws Exception {
        Node leader = startCluster(Duration.ofMillis(60), true);
        List<Node> followers = cluster.followers();

        // Replication is slower than the lock manager waits: the acquire command commits after the caller gave up.
        // Nobody refused the lock, so this is not "busy": the caller is told that the outcome is unknown.
        followers.forEach(follower -> cluster.delay(leader, follower, Duration.ofMillis(180)));
        assertThatThrownBy(() -> managers.get(leader).tryLock("orphan", Duration.ofMinutes(10)))
                .isInstanceOf(KurinTimeoutException.class)
                .hasMessageContaining("orphan");
        cluster.heal();

        // The compensating release frees the lock long before its ten minute time to live
        await().atMost(Duration.ofSeconds(10)).untilAsserted(() ->
                assertThat(leader.kurin().<LockRecord>submitQuery(new GetLock("orphan")).get(2, TimeUnit.SECONDS)).isNull());

        try (KurinLockManager patient = new KurinLockManager(followers.get(0).kurin(),
                followers.get(0).service(LockStateMachine.class), Duration.ofSeconds(5))) {
            assertThat(patient.tryLock("orphan", Duration.ofSeconds(30))).isPresent();
        }
    }

    // Spec test 14
    @Test
    void nestedReentrantUnlockDoesNotReleaseTheOuterLock() throws Exception {
        Node leader = startCluster(Duration.ofSeconds(5));
        KurinLockManager manager = managers.get(cluster.node(0));
        KurinLockManager other = managers.get(cluster.node(1));
        LockOptions reentrant = LockOptions.ttl(Duration.ofSeconds(30)).reentrant(true);

        LockHandle outer = manager.lock("inventory", reentrant);
        long logIndexAfterOuter = leader.status().lastLogIndex();
        LockHandle inner = manager.lock("inventory", reentrant);

        // The inner acquisition is local: same token, nothing written to the Raft log
        assertThat(inner.fencingToken()).isEqualTo(outer.fencingToken());
        assertThat(leader.status().lastLogIndex()).isEqualTo(logIndexAfterOuter);

        inner.release();
        assertThat(inner.isHeld()).isFalse();
        assertThat(outer.isHeld()).isTrue();
        assertThat(other.tryLock("inventory", Duration.ofSeconds(5))).isEmpty();

        outer.release();
        assertThat(outer.isHeld()).isFalse();
        Optional<LockHandle> next = other.tryLock("inventory", Duration.ofSeconds(5));
        assertThat(next).isPresent();
        assertThat(next.get().fencingToken()).isGreaterThan(outer.fencingToken());
    }

    @Test
    void nonReentrantSecondAcquisitionByTheSameThreadIsRefused() {
        startCluster(Duration.ofSeconds(5));
        KurinLockManager manager = managers.get(cluster.node(0));

        try (LockHandle held = manager.lock("plain", Duration.ofSeconds(30), Duration.ZERO)) {
            assertThat(manager.tryLock("plain", Duration.ofSeconds(30))).isEmpty();
            assertThatThrownBy(() -> manager.lock("plain", Duration.ofSeconds(30), Duration.ofMillis(200)))
                    .isInstanceOf(KurinLockAcquisitionException.class);
            assertThat(held.isHeld()).isTrue();
        }
    }

    @Test
    void waitingThreadGetsTheLockAsSoonAsItIsReleased() throws Exception {
        startCluster(Duration.ofSeconds(5));
        KurinLockManager holder = managers.get(cluster.node(0));
        KurinLockManager waiter = managers.get(cluster.node(1));
        workers = Executors.newSingleThreadExecutor();

        LockHandle held = holder.lock("handover", Duration.ofSeconds(30), Duration.ZERO);
        Future<LockHandle> waiting = workers.submit(() -> waiter.lock("handover", Duration.ofSeconds(30), Duration.ofSeconds(30)));
        // The waiter has tried once and is parked now
        await().atMost(Duration.ofSeconds(5)).until(() -> !waiting.isDone() && cluster.node(1).status().lastApplied() > 0);

        held.release();
        LockHandle handedOver = waiting.get(5, TimeUnit.SECONDS);
        assertThat(handedOver.fencingToken()).isGreaterThan(held.fencingToken());
        handedOver.release();
    }

    @Test
    void autoRenewKeepsTheLockBeyondItsTimeToLive() throws Exception {
        startCluster(Duration.ofSeconds(5));
        KurinLockManager manager = managers.get(cluster.node(0));
        KurinLockManager other = managers.get(cluster.node(1));

        LockHandle renewed = manager.lock("long-task", LockOptions.ttl(Duration.ofMillis(400)).autoRenew(true));
        long firstExpiry = renewed.expiresAt().toEpochMilli();

        // Well past the original expiry the lock is still held and still refused to others
        await().atMost(Duration.ofSeconds(10)).until(() -> System.currentTimeMillis() > firstExpiry + 400);
        assertThat(renewed.isHeld()).isTrue();
        assertThat(renewed.expiresAt().toEpochMilli()).isGreaterThan(firstExpiry);
        assertThat(other.tryLock("long-task", Duration.ofSeconds(5))).isEmpty();

        renewed.release();
        assertThat(other.tryLock("long-task", Duration.ofSeconds(5))).isPresent();
    }

    // ---------------------------------------------------------------- 1.1.2: "unknown" is not "busy"

    // Leaves one node running. It cannot elect itself and, once its election timer fired, knows no leader.
    private Node leaveOneNodeWithoutLeader() {
        Node survivor = cluster.node(0);
        cluster.kill(cluster.node(1));
        cluster.kill(cluster.node(2));
        await("the survivor notices that it has no leader").atMost(ELECTION)
                .until(() -> survivor.status().leaderId() == null && !survivor.status().isLeader());
        return survivor;
    }

    @Test
    void tryLockThrowsWhenTheClusterHasNoLeaderInsteadOfReportingBusy() {
        startCluster(Duration.ofSeconds(2));
        Node survivor = leaveOneNodeWithoutLeader();

        assertThatThrownBy(() -> managers.get(survivor).tryLock("ledger", Duration.ofSeconds(30)))
                .isInstanceOf(KurinNoLeaderException.class)
                .hasMessageContaining("ledger")
                .hasCauseInstanceOf(KurinNoLeaderException.class);
    }

    @Test
    void lockWithWaitKeepsTryingAndThenThrowsTheLastClusterError() {
        startCluster(Duration.ofSeconds(2));
        Node survivor = leaveOneNodeWithoutLeader();
        long startedNanos = System.nanoTime();

        assertThatThrownBy(() -> managers.get(survivor).lock("ledger", Duration.ofSeconds(30), Duration.ofMillis(700)))
                .isInstanceOf(KurinNoLeaderException.class)
                .isNotInstanceOf(KurinLockAcquisitionException.class);

        // It did not give up at the first failure: the whole wait was used
        assertThat(Duration.ofNanos(System.nanoTime() - startedNanos)).isGreaterThanOrEqualTo(Duration.ofMillis(700));
    }

    @Test
    void lockWithWaitSucceedsWhenTheClusterComesBackDuringTheWait() throws Exception {
        startCluster(Duration.ofSeconds(2));
        Node survivor = leaveOneNodeWithoutLeader();
        workers = Executors.newSingleThreadExecutor();

        Future<LockHandle> waiting = workers.submit(() ->
                managers.get(survivor).lock("ledger", Duration.ofSeconds(30), Duration.ofSeconds(60)));
        // Several attempts fail for lack of a leader before the cluster is whole again
        await().pollDelay(Duration.ofMillis(400)).atMost(Duration.ofSeconds(5)).until(() -> !waiting.isDone());
        cluster.restart(cluster.node(1));
        cluster.restart(cluster.node(2));

        LockHandle acquired = waiting.get(30, TimeUnit.SECONDS);
        assertThat(acquired.isHeld()).isTrue();
        acquired.release();
    }

    @Test
    void busyIsStillAnEmptyResultAndAnAcquisitionException() {
        startCluster(Duration.ofSeconds(5));
        KurinLockManager holder = managers.get(cluster.node(0));
        KurinLockManager other = managers.get(cluster.node(1));

        try (LockHandle held = holder.lock("ledger", Duration.ofSeconds(30), Duration.ZERO)) {
            assertThat(other.tryLock("ledger", Duration.ofSeconds(30))).isEmpty();
            assertThat(other.tryLock("ledger", Duration.ofSeconds(30), Duration.ofMillis(200))).isEmpty();
            assertThatThrownBy(() -> other.lock("ledger", Duration.ofSeconds(30), Duration.ofMillis(200)))
                    .isInstanceOf(KurinLockAcquisitionException.class)
                    .hasMessageContaining("200 ms");
            assertThat(held.isHeld()).isTrue();
        }
    }

    @Test
    void undoingAnAttemptWithUnknownOutcomeDoesNotReleaseTheLockOfTheNextAttempt() throws Exception {
        Node leader = startCluster(Duration.ofMillis(60), true);
        KurinLockManager manager = managers.get(leader);
        cluster.followers().forEach(follower -> cluster.delay(leader, follower, Duration.ofMillis(180)));
        assertThatThrownBy(() -> manager.tryLock("retried", Duration.ofMinutes(10))).isInstanceOf(KurinTimeoutException.class);
        cluster.heal();

        // The same caller tries again. Its first attempt is undone under its own owner id, in the background.
        try (KurinLockManager patient = new KurinLockManager(leader.kurin(), leader.service(LockStateMachine.class), Duration.ofSeconds(5))) {
            LockHandle second = patient.lock("retried", Duration.ofMinutes(10), Duration.ofSeconds(20));

            // However often the release of the first attempt is repeated, it never removes the second grant
            await().during(Duration.ofMillis(600)).atMost(Duration.ofSeconds(5)).until(() -> {
                LockRecord record = leader.kurin().<LockRecord>submitQuery(new GetLock("retried")).get(2, TimeUnit.SECONDS);
                return record != null && record.fencingToken() == second.fencingToken();
            });
            assertThat(second.isHeld()).isTrue();
        }
    }
}
