package org.kurin.kurinlocksspringbootstarter;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.kurin.api.LeadershipToken;
import org.kurin.kurinlocks.api.KurinLockContext;
import org.kurin.kurinlocks.job.JobCommands.JobRun;
import org.kurin.kurinlocks.job.JobCommands.JobStatus;
import org.kurin.kurinlocks.job.KurinJobContext;
import org.kurin.kurinlocks.job.KurinJobs;
import org.kurin.kurinlocks.leader.CancellationSignal;
import org.kurin.kurinlocks.leader.KurinLeadership;
import org.kurin.kurinlocksspringbootstarter.annotation.KurinDistributedLock;
import org.kurin.kurinlocksspringbootstarter.annotation.KurinLeaderOnly;
import org.kurin.kurinlocksspringbootstarter.annotation.KurinLeaderTask;
import org.kurin.kurinlocksspringbootstarter.annotation.KurinScheduledOnce;
import org.kurin.raft.KurinNode;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
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

class CoordinationClusterTest {

    /**
     * Shared by the three application contexts of a test, which stand for three instances of one application.
     */
    static final class Shared {
        static final Map<Long, List<String>> JOB_RUNS = new ConcurrentHashMap<>();
        static final Set<String> NODES_IN_LEADER_METHOD = ConcurrentHashMap.newKeySet();
        static final AtomicInteger LEADER_METHOD_OVERLAPS = new AtomicInteger();
        static final Map<String, AtomicInteger> LEADER_METHOD_RUNS = new ConcurrentHashMap<>();
        static final AtomicInteger INSIDE_LOCKED_METHOD = new AtomicInteger();
        static final AtomicInteger LOCK_OVERLAPS = new AtomicInteger();
        static final List<Long> LOCK_TOKENS = Collections.synchronizedList(new ArrayList<>());
        static final Map<String, AtomicInteger> LEADER_TASK_RUNS = new ConcurrentHashMap<>();
        static final List<LeadershipToken> LEADER_TASK_TOKENS = new CopyOnWriteArrayList<>();

        static void reset() {
            JOB_RUNS.clear();
            NODES_IN_LEADER_METHOD.clear();
            LEADER_METHOD_OVERLAPS.set(0);
            LEADER_METHOD_RUNS.clear();
            INSIDE_LOCKED_METHOD.set(0);
            LOCK_OVERLAPS.set(0);
            LOCK_TOKENS.clear();
            LEADER_TASK_RUNS.clear();
            LEADER_TASK_TOKENS.clear();
        }
    }

    @EnableAutoConfiguration
    @EnableScheduling
    static class App {
        @Bean
        Work work(KurinNode node) {
            return new Work(node);
        }
    }

    public static class Work {
        private final KurinNode node;

        public Work(KurinNode node) {
            this.node = node;
        }

        // Every instance triggers the job several times per slot; one instance may run it per slot
        @Scheduled(fixedRate = 40)
        @KurinScheduledOnce(name = "report", slot = "PT0.2S", lease = "PT5S")
        public void report() {
            Shared.JOB_RUNS.computeIfAbsent(KurinJobContext.current().slot(), slot -> new CopyOnWriteArrayList<>()).add(node.nodeId());
        }

        @KurinLeaderOnly
        public String leaderWork() throws InterruptedException {
            Shared.NODES_IN_LEADER_METHOD.add(node.nodeId());
            if (Shared.NODES_IN_LEADER_METHOD.size() > 1) {
                Shared.LEADER_METHOD_OVERLAPS.incrementAndGet();
            }
            // Stands for a few milliseconds of real work
            new CountDownLatch(1).await(3, TimeUnit.MILLISECONDS);
            Shared.LEADER_METHOD_RUNS.computeIfAbsent(node.nodeId(), id -> new AtomicInteger()).incrementAndGet();
            Shared.NODES_IN_LEADER_METHOD.remove(node.nodeId());
            return KurinLeadership.currentToken().orElseThrow().nodeId();
        }

        @KurinDistributedLock(key = "'transfer:' + #account", ttl = "PT30S", waitTimeout = "PT30S", onBusy = KurinDistributedLock.OnBusy.WAIT)
        public void transfer(String account) {
            if (Shared.INSIDE_LOCKED_METHOD.incrementAndGet() != 1) {
                Shared.LOCK_OVERLAPS.incrementAndGet();
            }
            Shared.LOCK_TOKENS.add(KurinLockContext.current().fencingToken());
            Shared.INSIDE_LOCKED_METHOD.decrementAndGet();
        }

        @KurinLeaderTask(name = "housekeeping", interval = "20ms")
        public void housekeeping(LeadershipToken token, CancellationSignal signal) {
            Shared.LEADER_TASK_RUNS.computeIfAbsent(node.nodeId(), id -> new AtomicInteger()).incrementAndGet();
            Shared.LEADER_TASK_TOKENS.add(token);
        }
    }

    private SpringCluster cluster;
    private ExecutorService callers;

    @BeforeEach
    void start() {
        Shared.reset();
        cluster = new SpringCluster(3, App.class);
        cluster.awaitLeader();
    }

    @AfterEach
    void stop() {
        if (callers != null) {
            callers.shutdownNow();
        }
        cluster.close();
    }

    // Spec test 15
    @Test
    void scheduledOnceRunsEachSlotOnExactlyOneInstance() {
        await().atMost(Duration.ofSeconds(20)).until(() -> Shared.JOB_RUNS.size() >= 10);

        assertThat(Shared.JOB_RUNS.values()).allSatisfy(runs -> assertThat(runs).hasSize(1));

        // The replicated history agrees with what actually ran
        List<JobRun> history = cluster.contexts().get(0).getBean(KurinJobs.class).history("report");
        assertThat(history).isNotEmpty().allSatisfy(run -> {
            if (run.status() == JobStatus.SUCCEEDED) {
                assertThat(Shared.JOB_RUNS.get(run.slot())).hasSize(1);
                assertThat(run.ownerId()).startsWith(Shared.JOB_RUNS.get(run.slot()).get(0));
            }
        });
    }

    // Spec test 16
    @Test
    void leaderOnlyMethodNeverRunsOnTwoInstancesAtOnceAcrossAPartition() throws Exception {
        ConfigurableApplicationContext oldLeader = cluster.leader().orElseThrow();
        String oldLeaderId = cluster.node(oldLeader).nodeId();
        AtomicBoolean stop = new AtomicBoolean();
        callers = Executors.newFixedThreadPool(cluster.contexts().size());

        List<Future<Integer>> loops = new ArrayList<>();
        for (ConfigurableApplicationContext context : cluster.contexts()) {
            Work work = context.getBean(Work.class);
            loops.add(callers.submit(() -> {
                int skipped = 0;
                while (!stop.get()) {
                    if (work.leaderWork() == null) {
                        skipped++;
                        Thread.onSpinWait();
                    }
                }
                return skipped;
            }));
        }

        await().atMost(Duration.ofSeconds(10)).until(() -> runsOf(oldLeaderId) >= 20);
        assertThat(Shared.LEADER_METHOD_RUNS.keySet()).containsExactly(oldLeaderId);

        // The leader is cut off while it is busy. It must stop before a successor starts.
        cluster.isolate(oldLeader);
        ConfigurableApplicationContext newLeader = cluster.awaitLeader();
        String newLeaderId = cluster.node(newLeader).nodeId();
        assertThat(newLeaderId).isNotEqualTo(oldLeaderId);
        await().atMost(Duration.ofSeconds(10)).until(() -> runsOf(newLeaderId) >= 20);
        int runsOfOldLeader = runsOf(oldLeaderId);

        cluster.heal();
        await().atMost(Duration.ofSeconds(10)).until(() -> newLeaderId.equals(cluster.node(oldLeader).status().leaderId()));
        stop.set(true);
        for (Future<Integer> loop : loops) {
            assertThat(loop.get(10, TimeUnit.SECONDS)).isPositive();
        }

        assertThat(Shared.LEADER_METHOD_OVERLAPS.get()).isZero();
        assertThat(runsOf(oldLeaderId)).isEqualTo(runsOfOldLeader);
        assertThat(Shared.LEADER_METHOD_RUNS.keySet()).containsExactlyInAnyOrder(oldLeaderId, newLeaderId);
    }

    private static int runsOf(String nodeId) {
        AtomicInteger runs = Shared.LEADER_METHOD_RUNS.get(nodeId);
        return runs != null ? runs.get() : 0;
    }

    @Test
    void distributedLockSerializesCallsAcrossInstances() throws Exception {
        callers = Executors.newFixedThreadPool(9);
        List<Future<?>> calls = new ArrayList<>();
        for (ConfigurableApplicationContext context : cluster.contexts()) {
            Work work = context.getBean(Work.class);
            for (int thread = 0; thread < 3; thread++) {
                calls.add(callers.submit(() -> {
                    for (int i = 0; i < 4; i++) {
                        work.transfer("ACC-1");
                    }
                }));
            }
        }
        for (Future<?> call : calls) {
            call.get(60, TimeUnit.SECONDS);
        }

        assertThat(Shared.LOCK_OVERLAPS.get()).isZero();
        assertThat(Shared.LOCK_TOKENS).hasSize(36).isSorted().doesNotHaveDuplicates();
    }

    @Test
    void leaderTaskRunsOnTheLeaderInstanceOnly() {
        String leaderId = cluster.node(cluster.leader().orElseThrow()).nodeId();

        await().atMost(Duration.ofSeconds(10)).until(() -> {
            AtomicInteger runs = Shared.LEADER_TASK_RUNS.get(leaderId);
            return runs != null && runs.get() >= 5;
        });

        assertThat(Shared.LEADER_TASK_RUNS.keySet()).containsExactly(leaderId);
        assertThat(Shared.LEADER_TASK_TOKENS).allSatisfy(token -> assertThat(token.nodeId()).isEqualTo(leaderId));
    }
}
