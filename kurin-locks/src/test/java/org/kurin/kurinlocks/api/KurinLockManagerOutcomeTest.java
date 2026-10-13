package org.kurin.kurinlocks.api;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.kurin.api.KurinInstrumentation;
import org.kurin.api.exception.KurinCommandFailedException;
import org.kurin.api.exception.KurinException;
import org.kurin.api.exception.KurinNoLeaderException;
import org.kurin.api.exception.KurinNotLeaderException;
import org.kurin.api.exception.KurinNotStartedException;
import org.kurin.api.exception.KurinRemoteException;
import org.kurin.api.exception.KurinSerializationException;
import org.kurin.api.exception.KurinStorageException;
import org.kurin.api.exception.KurinTimeoutException;
import org.kurin.kurinlocks.command.LockCommands.AcquireLock;
import org.kurin.kurinlocks.command.LockCommands.AcquireResult;
import org.kurin.kurinlocks.command.LockCommands.ReleaseLock;
import org.kurin.kurinlocks.exception.KurinLockAcquisitionException;
import org.kurin.kurinlocks.state.LockStateMachine;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

/**
 * What the lock manager makes of every kind of answer the cluster can give to an acquisition. The cluster is
 * scripted, so each failure is the exact one under test.
 */
class KurinLockManagerOutcomeTest {
    private static final Duration TTL = Duration.ofSeconds(30);
    private static final AcquireResult GRANTED = new AcquireResult(true, 7, 1_000);

    /**
     * Answers acquisitions from a script, one entry per attempt; the last entry answers all later attempts.
     */
    private static final class ScriptedCluster implements KurinLockManager.Cluster {
        final List<Supplier<CompletableFuture<?>>> acquisitions = new ArrayList<>();
        final List<AcquireLock> acquireCommands = new CopyOnWriteArrayList<>();
        final List<ReleaseLock> releaseCommands = new CopyOnWriteArrayList<>();
        final AtomicInteger attempt = new AtomicInteger();
        volatile Supplier<CompletableFuture<?>> releases = () -> CompletableFuture.completedFuture(true);
        volatile boolean started = true;

        ScriptedCluster answer(Object result) {
            acquisitions.add(() -> CompletableFuture.completedFuture(result));
            return this;
        }

        ScriptedCluster fail(Throwable error) {
            acquisitions.add(() -> CompletableFuture.failedFuture(error));
            return this;
        }

        ScriptedCluster silence() {
            acquisitions.add(CompletableFuture::new);
            return this;
        }

        @Override
        public String nodeId() {
            return "scripted:1";
        }

        @Override
        public boolean isStarted() {
            return started;
        }

        @Override
        @SuppressWarnings("unchecked")
        public <T> CompletableFuture<T> submitCommand(Object command, Duration timeout) {
            if (command instanceof AcquireLock acquire) {
                acquireCommands.add(acquire);
                int index = Math.min(attempt.getAndIncrement(), acquisitions.size() - 1);
                return (CompletableFuture<T>) acquisitions.get(index).get();
            }
            releaseCommands.add((ReleaseLock) command);
            return (CompletableFuture<T>) releases.get();
        }

        @Override
        public KurinInstrumentation.Span startSpan(String name) {
            return KurinInstrumentation.Span.NOOP;
        }

        List<String> releasedOwners() {
            return releaseCommands.stream().map(ReleaseLock::ownerId).toList();
        }
    }

    private static final class RecordingListener implements KurinLockManager.LockListener {
        final List<String> events = new CopyOnWriteArrayList<>();

        @Override
        public void onAcquire(String key, boolean acquired, Duration waited) {
            events.add((acquired ? "acquired " : "busy ") + key);
        }

        @Override
        public void onRelease(String key) {
            events.add("released " + key);
        }

        @Override
        public void onAcquireFailed(String key, Duration waited, KurinException cause) {
            events.add("unknown " + key + " " + cause.getClass().getSimpleName());
        }

        @Override
        public void onReleaseUnconfirmed(String key) {
            events.add("unconfirmed " + key);
        }
    }

    private KurinLockManager manager;
    private ExecutorService worker;

    @AfterEach
    void close() {
        if (manager != null) {
            manager.close();
        }
        if (worker != null) {
            worker.shutdownNow();
        }
    }

    private KurinLockManager manager(ScriptedCluster cluster) {
        return manager(cluster, Duration.ofSeconds(2));
    }

    private KurinLockManager manager(ScriptedCluster cluster, Duration commandTimeout) {
        manager = new KurinLockManager(cluster, new LockStateMachine(), commandTimeout);
        return manager;
    }

    // ---------------------------------------------------------------- definite answers

    @Test
    void grantGivesAHandleAndReleaseNamesTheOwnerOfTheAttempt() {
        ScriptedCluster cluster = new ScriptedCluster().answer(GRANTED);
        RecordingListener listener = new RecordingListener();
        KurinLockManager locks = manager(cluster);
        locks.addListener(listener);

        LockHandle handle = locks.tryLock("ledger", TTL).orElseThrow();

        assertThat(handle.fencingToken()).isEqualTo(7);
        assertThat(handle.isHeld()).isTrue();
        handle.release();
        assertThat(cluster.releasedOwners()).containsExactly(cluster.acquireCommands.get(0).ownerId());
        assertThat(listener.events).containsExactly("acquired ledger", "released ledger");
    }

    @Test
    void refusalIsBusyAnEmptyResultWithNothingToUndo() {
        ScriptedCluster cluster = new ScriptedCluster().answer(AcquireResult.denied());
        RecordingListener listener = new RecordingListener();
        KurinLockManager locks = manager(cluster);
        locks.addListener(listener);

        assertThat(locks.tryLock("ledger", TTL)).isEmpty();
        assertThatThrownBy(() -> locks.lock("ledger", TTL, Duration.ofMillis(120)))
                .isExactlyInstanceOf(KurinLockAcquisitionException.class)
                .hasMessageContaining("held by another owner")
                .hasMessageContaining("120 ms");

        assertThat(cluster.releaseCommands).isEmpty();
        assertThat(listener.events).containsExactly("busy ledger", "busy ledger");
    }

    // ---------------------------------------------------------------- no definite answer

    @Test
    void noLeaderIsReportedAsSuchAndNothingIsUndoneBecauseNothingWasSubmitted() {
        KurinNoLeaderException original = new KurinNoLeaderException("The cluster has no known leader yet");
        ScriptedCluster cluster = new ScriptedCluster().fail(original);
        RecordingListener listener = new RecordingListener();
        KurinLockManager locks = manager(cluster);
        locks.addListener(listener);

        assertThatThrownBy(() -> locks.tryLock("ledger", TTL))
                .isExactlyInstanceOf(KurinNoLeaderException.class)
                .hasMessageContaining("Lock 'ledger'")
                .hasMessageContaining("no known leader")
                .hasCause(original);

        assertThat(cluster.releaseCommands).isEmpty();
        assertThat(listener.events).containsExactly("unknown ledger KurinNoLeaderException");
    }

    @Test
    void unreachableNodeIsReportedAsRemoteFailureAndTheAttemptIsUndone() {
        KurinRemoteException original = new KurinRemoteException("connection closed");
        ScriptedCluster cluster = new ScriptedCluster().fail(original);
        KurinLockManager locks = manager(cluster);

        assertThatThrownBy(() -> locks.tryLock("ledger", TTL))
                .isExactlyInstanceOf(KurinRemoteException.class)
                .hasMessageContaining("Lock 'ledger'")
                .hasCause(original);

        // The release carries the owner id of exactly that attempt
        await().atMost(Duration.ofSeconds(5)).untilAsserted(() ->
                assertThat(cluster.releasedOwners()).containsExactly(cluster.acquireCommands.get(0).ownerId()));
    }

    @Test
    void timeoutReportedByTheNodeStaysATimeout() {
        KurinTimeoutException original = new KurinTimeoutException("Command AcquireLock got no result within 2000 ms");
        ScriptedCluster cluster = new ScriptedCluster().fail(original);
        KurinLockManager locks = manager(cluster);

        assertThatThrownBy(() -> locks.tryLock("ledger", TTL))
                .isExactlyInstanceOf(KurinTimeoutException.class)
                .hasMessageContaining("Lock 'ledger'")
                .hasCause(original);
        assertThat(cluster.releaseCommands).hasSize(1);
    }

    @Test
    void nodeThatNeverAnswersIsATimeoutToo() {
        ScriptedCluster cluster = new ScriptedCluster().silence();
        KurinLockManager locks = manager(cluster, Duration.ofMillis(50));

        assertThatThrownBy(() -> locks.tryLock("ledger", TTL))
                .isExactlyInstanceOf(KurinTimeoutException.class)
                .hasMessageContaining("Lock 'ledger'")
                .hasCauseInstanceOf(TimeoutException.class);
        assertThat(cluster.releaseCommands).hasSize(1);
    }

    @Test
    void otherClusterErrorsReachTheCallerUnchanged() {
        for (KurinException original : List.of(
                new KurinNotLeaderException("Node a:1 is not the leader", "b:2"),
                new KurinStorageException("Node a:1 cannot write its durable state"))) {
            ScriptedCluster cluster = new ScriptedCluster().fail(original);
            try (KurinLockManager locks = new KurinLockManager(cluster, new LockStateMachine(), Duration.ofSeconds(2))) {
                assertThatThrownBy(() -> locks.tryLock("ledger", TTL)).isSameAs(original);
                assertThat(cluster.releaseCommands).hasSize(1);
            }
        }
    }

    @Test
    void unexpectedFailureIsWrappedInAKurinException() {
        IllegalStateException original = new IllegalStateException("boom");
        ScriptedCluster cluster = new ScriptedCluster().fail(original);
        KurinLockManager locks = manager(cluster);

        assertThatThrownBy(() -> locks.lock("ledger", TTL, Duration.ofSeconds(30)))
                .isExactlyInstanceOf(KurinException.class)
                .hasMessageContaining("Lock 'ledger'")
                .hasCause(original);
        // Nothing suggests that another attempt would fare better
        assertThat(cluster.acquireCommands).hasSize(1);
    }

    // ---------------------------------------------------------------- waiting

    @Test
    void failuresThatWaitingCannotCureAreThrownAtOnce() {
        for (KurinException original : List.of(
                new KurinNotStartedException("Node a:1 is shut down"),
                new KurinSerializationException("Type is not registered"),
                new KurinCommandFailedException("java.lang.IllegalStateException", "handler failed"))) {
            ScriptedCluster cluster = new ScriptedCluster().fail(original);
            try (KurinLockManager locks = new KurinLockManager(cluster, new LockStateMachine(), Duration.ofSeconds(2))) {
                assertThatThrownBy(() -> locks.lock("ledger", TTL, Duration.ofSeconds(30))).isSameAs(original);
                assertThat(cluster.acquireCommands).hasSize(1);
            }
        }
    }

    @Test
    void waitRetriesAfterUnknownOutcomesAndEveryAttemptHasItsOwnOwner() {
        ScriptedCluster cluster = new ScriptedCluster()
                .fail(new KurinRemoteException("connection closed"))
                .fail(new KurinTimeoutException("no result"))
                .answer(GRANTED);
        RecordingListener listener = new RecordingListener();
        KurinLockManager locks = manager(cluster);
        locks.addListener(listener);

        LockHandle handle = locks.lock("ledger", TTL, Duration.ofSeconds(30));

        assertThat(handle.isHeld()).isTrue();
        List<String> owners = cluster.acquireCommands.stream().map(AcquireLock::ownerId).toList();
        assertThat(owners).hasSize(3).doesNotHaveDuplicates();
        // The two attempts without an answer are undone; the one that was granted never is
        await().atMost(Duration.ofSeconds(5)).untilAsserted(() ->
                assertThat(cluster.releasedOwners()).containsExactlyInAnyOrder(owners.get(0), owners.get(1)));
        assertThat(listener.events).containsExactly("acquired ledger");
    }

    @Test
    void whenTheWaitEndsTheLastAttemptDecidesUnknown() {
        ScriptedCluster cluster = new ScriptedCluster()
                .answer(AcquireResult.denied())
                .fail(new KurinRemoteException("connection closed"));
        RecordingListener listener = new RecordingListener();
        KurinLockManager locks = manager(cluster);
        locks.addListener(listener);

        assertThatThrownBy(() -> locks.lock("ledger", TTL, Duration.ofMillis(250)))
                .isExactlyInstanceOf(KurinRemoteException.class);

        assertThat(cluster.acquireCommands.size()).isGreaterThanOrEqualTo(2);
        assertThat(listener.events).containsExactly("unknown ledger KurinRemoteException");
    }

    @Test
    void whenTheWaitEndsTheLastAttemptDecidesBusy() {
        ScriptedCluster cluster = new ScriptedCluster()
                .fail(new KurinRemoteException("connection closed"))
                .answer(AcquireResult.denied());
        KurinLockManager locks = manager(cluster);

        assertThat(locks.tryLock("ledger", TTL, Duration.ofMillis(250))).isEmpty();
        assertThatThrownBy(() -> locks.lock("ledger", TTL, Duration.ofMillis(100)))
                .isExactlyInstanceOf(KurinLockAcquisitionException.class);
    }

    // ---------------------------------------------------------------- undoing an attempt

    @Test
    void releaseThatCannotBeConfirmedIsRetriedAndThenReported() {
        ScriptedCluster cluster = new ScriptedCluster().fail(new KurinRemoteException("connection closed"));
        cluster.releases = () -> CompletableFuture.failedFuture(new KurinRemoteException("still closed"));
        RecordingListener listener = new RecordingListener();
        KurinLockManager locks = manager(cluster);
        locks.addListener(listener);

        assertThatThrownBy(() -> locks.tryLock("ledger", TTL)).isInstanceOf(KurinRemoteException.class);

        await().atMost(Duration.ofSeconds(10)).untilAsserted(() ->
                assertThat(listener.events).contains("unconfirmed ledger"));
        assertThat(cluster.releaseCommands).hasSize(5);
        assertThat(cluster.releasedOwners()).containsOnly(cluster.acquireCommands.get(0).ownerId());
    }

    @Test
    void releaseIsNotRetriedOnceTheNodeIsStopped() {
        ScriptedCluster cluster = new ScriptedCluster().fail(new KurinRemoteException("connection closed"));
        cluster.releases = () -> CompletableFuture.failedFuture(new KurinNotStartedException("Node is shut down"));
        cluster.started = false;
        RecordingListener listener = new RecordingListener();
        KurinLockManager locks = manager(cluster);
        locks.addListener(listener);

        assertThatThrownBy(() -> locks.tryLock("ledger", TTL)).isInstanceOf(KurinRemoteException.class);

        assertThat(cluster.releaseCommands).hasSize(1);
        assertThat(listener.events).contains("unconfirmed ledger");
    }

    @Test
    void releaseIsReportedWhenTheManagerIsClosedBeforeItCanBeRetried() {
        ScriptedCluster cluster = new ScriptedCluster().fail(new KurinRemoteException("connection closed"));
        cluster.releases = () -> CompletableFuture.failedFuture(new KurinRemoteException("still closed"));
        RecordingListener listener = new RecordingListener();
        KurinLockManager locks = manager(cluster);
        locks.addListener(listener);
        locks.close();

        assertThatThrownBy(() -> locks.tryLock("ledger", TTL)).isInstanceOf(KurinRemoteException.class);

        assertThat(cluster.releaseCommands).hasSize(1);
        assertThat(listener.events).contains("unconfirmed ledger");
    }

    @Test
    void interruptedAcquisitionIsUndoneAndReportedAsInterrupted() throws Exception {
        ScriptedCluster cluster = new ScriptedCluster().silence();
        KurinLockManager locks = manager(cluster);
        worker = Executors.newSingleThreadExecutor();

        Future<Optional<LockHandle>> acquiring = worker.submit(() -> locks.tryLock("ledger", TTL));
        await().atMost(Duration.ofSeconds(5)).until(() -> cluster.acquireCommands.size() == 1);
        worker.shutdownNow();

        assertThatThrownBy(() -> acquiring.get(5, TimeUnit.SECONDS))
                .hasCauseExactlyInstanceOf(KurinLockAcquisitionException.class)
                .hasMessageContaining("Interrupted");
        assertThat(cluster.releasedOwners()).containsExactly(cluster.acquireCommands.get(0).ownerId());
    }
}
