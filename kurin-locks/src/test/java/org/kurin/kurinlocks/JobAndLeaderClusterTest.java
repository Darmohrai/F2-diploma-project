package org.kurin.kurinlocks;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.kurin.api.LeadershipToken;
import org.kurin.kurinlocks.job.JobCommands.JobMode;
import org.kurin.kurinlocks.job.JobCommands.JobRun;
import org.kurin.kurinlocks.job.JobCommands.JobStatus;
import org.kurin.kurinlocks.job.JobStateMachine;
import org.kurin.kurinlocks.job.KurinJobContext;
import org.kurin.kurinlocks.job.KurinJobs;
import org.kurin.kurinlocks.leader.KurinLeaderTasks;
import org.kurin.kurinlocks.leader.KurinLeadership;
import org.kurin.kurinlocks.state.LockStateMachine;
import org.kurin.test.KurinTestCluster;
import org.kurin.test.KurinTestCluster.Node;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

class JobAndLeaderClusterTest {
    private static final Duration ELECTION = Duration.ofSeconds(10);

    private KurinTestCluster cluster;
    private final Map<Node, KurinJobs> jobs = new HashMap<>();
    private final List<AutoCloseable> closeables = new ArrayList<>();
    private ExecutorService workers;

    @AfterEach
    void stop() throws Exception {
        if (workers != null) {
            workers.shutdownNow();
        }
        for (AutoCloseable closeable : closeables) {
            closeable.close();
        }
        if (cluster != null) {
            cluster.close();
        }
    }

    private void startCluster() {
        cluster = KurinTestCluster.builder().nodes(3)
                .services(() -> List.of(new LockStateMachine(), new JobStateMachine()))
                .build()
                .start();
        cluster.awaitLeader(ELECTION);
        for (Node node : cluster.nodes()) {
            jobs.put(node, new KurinJobs(node.kurin(), Duration.ofSeconds(3), 50));
        }
    }

    // Spec test 15, programmatic part: every node triggers every slot, one node runs it
    @Test
    void eachSlotIsExecutedByExactlyOneNode() throws Exception {
        startCluster();
        workers = Executors.newFixedThreadPool(3);
        Duration slot = Duration.ofMillis(150);
        Map<Long, AtomicInteger> executionsPerSlot = new ConcurrentHashMap<>();
        Map<Long, Long> tokenPerSlot = new ConcurrentHashMap<>();

        long lastSlot = Long.MIN_VALUE;
        while (executionsPerSlot.size() < 10) {
            long previous = lastSlot;
            // Next round when the wall clock reaches a new slot
            await().atMost(Duration.ofSeconds(5)).pollInterval(Duration.ofMillis(5))
                    .until(() -> KurinJobs.slotOf(System.currentTimeMillis(), slot) > previous);
            lastSlot = KurinJobs.slotOf(System.currentTimeMillis(), slot);

            List<Future<Optional<String>>> round = new ArrayList<>();
            for (Node node : cluster.nodes()) {
                round.add(workers.submit(() -> jobs.get(node).runOnce("report", slot, JobMode.AT_MOST_ONCE, Duration.ofSeconds(5), () -> {
                    KurinJobContext context = KurinJobContext.current();
                    executionsPerSlot.computeIfAbsent(context.slot(), key -> new AtomicInteger()).incrementAndGet();
                    tokenPerSlot.put(context.slot(), context.fencingToken());
                    return node.nodeId();
                })));
            }
            for (Future<Optional<String>> result : round) {
                result.get(10, TimeUnit.SECONDS);
            }
        }

        assertThat(executionsPerSlot.values()).allSatisfy(count -> assertThat(count.get()).isEqualTo(1));

        List<JobRun> history = jobs.get(cluster.node(0)).history("report");
        assertThat(history).extracting(JobRun::slot).containsExactlyElementsOf(executionsPerSlot.keySet().stream().sorted().toList());
        assertThat(history).allSatisfy(run -> {
            assertThat(run.status()).isEqualTo(JobStatus.SUCCEEDED);
            assertThat(run.fencingToken()).isEqualTo(tokenPerSlot.get(run.slot()));
        });
        assertThat(history).extracting(JobRun::fencingToken).isSorted().doesNotHaveDuplicates();
    }

    // Spec test 15, takeover: AT_LEAST_ONCE lets another node rerun a slot whose owner died
    @Test
    void abandonedSlotIsTakenOverOnlyInAtLeastOnceMode() throws Exception {
        startCluster();
        workers = Executors.newFixedThreadPool(2);
        // One slot for the whole test: all claims compete for the same one
        Duration slot = Duration.ofDays(365);
        Duration lease = Duration.ofMillis(400);
        Node doomed = cluster.followers().get(0);
        Node survivor = cluster.followers().get(1);
        CountDownLatch release = new CountDownLatch(1);
        CountDownLatch running = new CountDownLatch(2);

        for (String job : List.of("at-least-once", "at-most-once")) {
            JobMode mode = job.equals("at-least-once") ? JobMode.AT_LEAST_ONCE : JobMode.AT_MOST_ONCE;
            workers.submit(() -> jobs.get(doomed).runOnce(job, slot, mode, lease, () -> {
                running.countDown();
                release.await();
                return null;
            }));
        }
        assertThat(running.await(10, TimeUnit.SECONDS)).isTrue();
        JobRun firstRun = jobs.get(survivor).history("at-least-once").get(0);
        assertThat(firstRun.status()).isEqualTo(JobStatus.CLAIMED);
        assertThat(firstRun.ownerId()).startsWith(doomed.nodeId());

        // While the owner lives and its lease runs, nobody else gets the slot
        assertThat(jobs.get(survivor).runOnce("at-least-once", slot, JobMode.AT_LEAST_ONCE, lease, () -> "early")).isEmpty();

        cluster.kill(doomed);
        release.countDown();
        cluster.awaitLeader(ELECTION);

        AtomicInteger reruns = new AtomicInteger();
        await().atMost(Duration.ofSeconds(10)).pollInterval(Duration.ofMillis(50)).until(() ->
                jobs.get(survivor).runOnce("at-least-once", slot, JobMode.AT_LEAST_ONCE, lease, () -> {
                    reruns.incrementAndGet();
                    return "rerun";
                }).isPresent());

        JobRun secondRun = jobs.get(survivor).history("at-least-once").get(0);
        assertThat(reruns.get()).isEqualTo(1);
        assertThat(secondRun.ownerId()).startsWith(survivor.nodeId());
        assertThat(secondRun.status()).isEqualTo(JobStatus.SUCCEEDED);
        assertThat(secondRun.fencingToken()).isGreaterThan(firstRun.fencingToken());

        // The lease of the at-most-once claim is over as well, yet that slot stays with its dead owner
        assertThat(jobs.get(survivor).runOnce("at-most-once", slot, JobMode.AT_MOST_ONCE, lease, () -> "never")).isEmpty();
        assertThat(jobs.get(survivor).history("at-most-once").get(0).ownerId()).startsWith(doomed.nodeId());
    }

    @Test
    void failedJobIsRecordedAndItsExceptionPropagates() throws Exception {
        startCluster();
        KurinJobs node = jobs.get(cluster.node(0));

        org.assertj.core.api.Assertions.assertThatThrownBy(() ->
                        node.runOnce("flaky", Duration.ofDays(365), JobMode.AT_LEAST_ONCE, Duration.ofSeconds(5), () -> {
                            throw new IllegalStateException("disk full");
                        }))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("disk full");

        JobRun run = node.history("flaky").get(0);
        assertThat(run.status()).isEqualTo(JobStatus.FAILED);
        assertThat(run.error()).contains("disk full");
        // A reported failure is a completed run: the slot is not retried
        assertThat(node.runOnce("flaky", Duration.ofDays(365), JobMode.AT_LEAST_ONCE, Duration.ofSeconds(5), () -> "again")).isEmpty();
    }

    @Test
    void historyKeepsOnlyTheConfiguredNumberOfSlotsAndPrunedSlotsStayClaimed() throws Exception {
        startCluster();
        KurinJobs small = new KurinJobs(cluster.node(0).kurin(), Duration.ofSeconds(3), 3);
        Duration slot = Duration.ofMillis(40);

        Set<Long> executed = ConcurrentHashMap.newKeySet();
        while (executed.size() < 6) {
            small.runOnce("rolling", slot, JobMode.AT_MOST_ONCE, Duration.ofSeconds(5), () -> executed.add(KurinJobContext.current().slot()));
        }

        List<JobRun> history = small.history("rolling");
        assertThat(history).hasSize(3);
        assertThat(history).extracting(JobRun::slot).isSorted();
        assertThat(history.get(0).slot()).isGreaterThan(executed.stream().min(Long::compare).orElseThrow());
    }

    @Test
    void leaderTasksRunOnTheLeaderOnlyAndMoveWithLeadership() throws Exception {
        startCluster();
        Map<Node, AtomicInteger> runs = new HashMap<>();
        List<LeadershipToken> tokens = new CopyOnWriteArrayList<>();
        AtomicBoolean cancellationSeen = new AtomicBoolean();
        Node firstLeader = cluster.leader().orElseThrow();

        for (Node node : cluster.nodes()) {
            AtomicInteger counter = new AtomicInteger();
            runs.put(node, counter);
            KurinLeaderTasks tasks = new KurinLeaderTasks(node.kurin(), Duration.ofMillis(10));
            closeables.add(tasks);
            tasks.register("tick", Duration.ofMillis(20), (token, signal) -> {
                counter.incrementAndGet();
                tokens.add(token);
                assertThat(KurinLeadership.currentToken()).contains(token);
                if (node == firstLeader && counter.get() == 3) {
                    // Stays in the task until leadership is taken away
                    CountDownLatch never = new CountDownLatch(1);
                    try {
                        never.await();
                    } catch (InterruptedException e) {
                        cancellationSeen.set(signal.isCancelled());
                        throw e;
                    }
                }
            });
            tasks.start();
        }

        await().atMost(Duration.ofSeconds(5)).until(() -> runs.get(firstLeader).get() >= 3);
        assertThat(cluster.followers()).allSatisfy(follower -> assertThat(runs.get(follower).get()).isZero());

        // Cut the leader off: it loses its lease, its running task is cancelled, a new leader takes the task over
        cluster.partition(Set.of(firstLeader), Set.copyOf(cluster.followers()));
        await().atMost(Duration.ofSeconds(5)).untilTrue(cancellationSeen);
        Node secondLeader = cluster.awaitLeader(ELECTION);
        await().atMost(Duration.ofSeconds(5)).until(() -> runs.get(secondLeader).get() >= 2);

        int frozen = runs.get(firstLeader).get();
        assertThat(frozen).isEqualTo(3);
        assertThat(tokens).extracting(LeadershipToken::nodeId).containsOnly(firstLeader.nodeId(), secondLeader.nodeId());
        assertThat(tokens.get(tokens.size() - 1).term()).isGreaterThan(tokens.get(0).term());
    }
}
