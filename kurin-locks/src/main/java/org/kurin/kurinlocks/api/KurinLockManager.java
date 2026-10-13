package org.kurin.kurinlocks.api;

import org.kurin.api.KurinInstrumentation;
import org.kurin.api.exception.KurinCommandFailedException;
import org.kurin.api.exception.KurinException;
import org.kurin.api.exception.KurinNoLeaderException;
import org.kurin.api.exception.KurinNotStartedException;
import org.kurin.api.exception.KurinRemoteException;
import org.kurin.api.exception.KurinSerializationException;
import org.kurin.api.exception.KurinTimeoutException;
import org.kurin.kurinlocks.command.LockCommands.AcquireLock;
import org.kurin.kurinlocks.command.LockCommands.AcquireResult;
import org.kurin.kurinlocks.command.LockCommands.ReleaseLock;
import org.kurin.kurinlocks.command.LockCommands.RenewLock;
import org.kurin.kurinlocks.command.LockCommands.RenewResult;
import org.kurin.kurinlocks.exception.KurinLockAcquisitionException;
import org.kurin.kurinlocks.state.LockStateMachine;
import org.kurin.raft.KurinNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Cluster-wide locks backed by the replicated {@link LockStateMachine}.
 * <p>
 * Guarantees: at any moment the state machine records at most one owner per key, and every grant carries a
 * fencing token larger than all earlier ones. A lock is a lease of {@code ttl}: a holder that stalls longer
 * than that (a long pause, a partition) may keep working after the lock was granted to someone else, so
 * resources that must never see two writers should check the fencing token.
 * <p>
 * "Busy" and "unknown" are different outcomes. An acquisition comes back empty (or, from {@code lock}, with a
 * {@link KurinLockAcquisitionException}) only when the state machine answered that another owner holds the lock.
 * When the cluster gave no answer (no leader, a node out of reach, no reply in time) the caller gets the
 * {@link KurinException} that says why, and the attempt is released in the background in case it was granted
 * after all.
 */
public class KurinLockManager implements AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(KurinLockManager.class);

    private static final long BACKOFF_MIN_MS = 50;
    private static final long BACKOFF_MAX_MS = 1000;
    private static final int COMPENSATION_ATTEMPTS = 5;

    private final Cluster cluster;
    private final Duration commandTimeout;
    private final ScheduledExecutorService scheduler;
    private final Map<String, List<CompletableFuture<Void>>> waiters = new ConcurrentHashMap<>();
    private final ThreadLocal<Map<String, ReentrantHold>> reentrantHolds = ThreadLocal.withInitial(HashMap::new);
    private final List<LockListener> listeners = new CopyOnWriteArrayList<>();

    /**
     * Observes lock operations of this manager, for metrics.
     */
    public interface LockListener {
        /**
         * @param waited time from the first attempt to the outcome
         */
        void onAcquire(String key, boolean acquired, Duration waited);

        void onRelease(String key);

        /**
         * The cluster gave no definite answer, so the lock is neither acquired nor known to be busy.
         *
         * @param waited time from the first attempt to the failure
         * @param cause  what the caller is told
         */
        default void onAcquireFailed(String key, Duration waited, KurinException cause) {
        }

        /**
         * A release could not be confirmed after all its retries: the release of a handle, or the release that
         * undoes an attempt with an unknown outcome. If the lock is recorded, it stays until its time to live ends.
         */
        default void onReleaseUnconfirmed(String key) {
        }
    }

    /**
     * What the manager needs from the node it runs on. Tests put a scripted cluster behind it.
     */
    interface Cluster {
        String nodeId();

        boolean isStarted();

        <T> CompletableFuture<T> submitCommand(Object command, Duration timeout);

        KurinInstrumentation.Span startSpan(String name);
    }

    private record NodeCluster(KurinNode node) implements Cluster {
        @Override
        public String nodeId() {
            return node.nodeId();
        }

        @Override
        public boolean isStarted() {
            return node.isStarted();
        }

        @Override
        public <T> CompletableFuture<T> submitCommand(Object command, Duration timeout) {
            return node.submitCommand(command, timeout);
        }

        @Override
        public KurinInstrumentation.Span startSpan(String name) {
            // The key is not a tag: it is unbounded, and tags must stay few
            return node.startSpan(name, Map.of());
        }
    }

    private static final class ReentrantHold {
        final ManagedLock lock;
        int count = 1;

        ReentrantHold(ManagedLock lock) {
            this.lock = lock;
        }
    }

    /**
     * @param localStateMachine the lock state machine registered on this node; its local release events wake
     *                          threads waiting for a lock
     * @param commandTimeout    how long to wait for the cluster to answer a single lock command
     */
    public KurinLockManager(KurinNode node, LockStateMachine localStateMachine, Duration commandTimeout) {
        this(new NodeCluster(node), localStateMachine, commandTimeout);
    }

    KurinLockManager(Cluster cluster, LockStateMachine localStateMachine, Duration commandTimeout) {
        this.cluster = cluster;
        this.commandTimeout = commandTimeout;
        AtomicInteger threadNumber = new AtomicInteger();
        ScheduledThreadPoolExecutor executor = new ScheduledThreadPoolExecutor(2, task -> {
            Thread thread = new Thread(task, "kurin-locks-" + threadNumber.incrementAndGet());
            thread.setDaemon(true);
            return thread;
        });
        executor.setRemoveOnCancelPolicy(true);
        this.scheduler = executor;
        localStateMachine.addReleaseListener(this::onLockReleased);
    }

    public void addListener(LockListener listener) {
        listeners.add(listener);
    }

    // ---------------------------------------------------------------- acquiring

    /**
     * One attempt to take the lock.
     *
     * @return the handle, or empty if another owner holds the lock
     * @throws KurinException if the cluster gave no definite answer; see {@link #tryLock(String, LockOptions)}
     */
    public Optional<LockHandle> tryLock(String key, Duration ttl) {
        return tryLock(key, LockOptions.ttl(ttl));
    }

    /**
     * Tries to take the lock, waiting up to {@code waitTimeout} for the current holder to let go.
     *
     * @return the handle, or empty if another owner still held the lock when the wait ended
     * @throws KurinException if the cluster gave no definite answer; see {@link #tryLock(String, LockOptions)}
     */
    public Optional<LockHandle> tryLock(String key, Duration ttl, Duration waitTimeout) {
        return tryLock(key, LockOptions.ttl(ttl).waitTimeout(waitTimeout));
    }

    /**
     * @throws KurinLockAcquisitionException if another owner still held the lock when {@code waitTimeout} ended
     * @throws KurinException                if the cluster gave no definite answer; see
     *                                       {@link #tryLock(String, LockOptions)}
     */
    public LockHandle lock(String key, Duration ttl, Duration waitTimeout) {
        return lock(key, LockOptions.ttl(ttl).waitTimeout(waitTimeout));
    }

    /**
     * @throws KurinLockAcquisitionException if another owner still held the lock when the wait ended
     * @throws KurinException                if the cluster gave no definite answer; see
     *                                       {@link #tryLock(String, LockOptions)}
     */
    public LockHandle lock(String key, LockOptions options) {
        return tryLock(key, options).orElseThrow(() -> new KurinLockAcquisitionException(
                "Lock '" + key + "' is held by another owner; it was not acquired within "
                        + options.waitTimeout().toMillis() + " ms"));
    }

    /**
     * Takes the lock, trying again until {@code options.waitTimeout()} has passed. What the last attempt found
     * decides the outcome:
     * <ul>
     * <li>another owner holds the lock: empty;</li>
     * <li>the cluster gave no definite answer: the {@link KurinException} of that attempt, with its cause.
     * {@link KurinNoLeaderException} when there is no leader, {@link KurinRemoteException} when a node could not
     * be reached, {@link KurinTimeoutException} when no answer came in time. The lock may have been granted to
     * the attempt; it is released in the background, and a later attempt never depends on it.</li>
     * </ul>
     * A failed attempt is not a reason to stop early: the cluster usually has a leader again within an election
     * timeout, so the wait is used in full. With a wait of zero there is one attempt. Failures that waiting
     * cannot cure (the node is shut down, the command cannot be serialized) are thrown at once.
     *
     * @throws KurinLockAcquisitionException if the thread is interrupted while it waits
     */
    public Optional<LockHandle> tryLock(String key, LockOptions options) {
        if (key == null || key.isEmpty()) {
            throw new IllegalArgumentException("Lock key must not be empty");
        }
        if (options.reentrant()) {
            ReentrantHold hold = reentrantHolds.get().get(key);
            if (hold != null && hold.lock.isHeld()) {
                // Already ours on this thread: no cluster round trip, just one more level
                hold.count++;
                return Optional.of(new ReentrantView(hold));
            }
        }

        long startedNanos = System.nanoTime();
        KurinInstrumentation.Span span = cluster.startSpan("kurin.lock.acquire");
        Optional<ManagedLock> acquired;
        try {
            acquired = acquireWithWait(key, options, startedNanos);
        } catch (RuntimeException e) {
            span.end(e);
            if (e instanceof UnknownOutcome unknown) {
                Duration waited = Duration.ofNanos(System.nanoTime() - startedNanos);
                listeners.forEach(listener -> listener.onAcquireFailed(key, waited, unknown.reported));
                throw unknown.reported;
            }
            throw e;
        }
        span.end(null);
        Duration waited = Duration.ofNanos(System.nanoTime() - startedNanos);
        listeners.forEach(listener -> listener.onAcquire(key, acquired.isPresent(), waited));
        if (acquired.isEmpty()) {
            return Optional.empty();
        }

        ManagedLock lock = acquired.get();
        if (options.autoRenew()) {
            lock.startAutoRenew(options.ttl());
        }
        if (options.reentrant()) {
            ReentrantHold hold = new ReentrantHold(lock);
            reentrantHolds.get().put(key, hold);
            return Optional.of(new ReentrantView(hold));
        }
        return Optional.of(lock);
    }

    private Optional<ManagedLock> acquireWithWait(String key, LockOptions options, long startedNanos) {
        long deadlineNanos = startedNanos + options.waitTimeout().toNanos();
        long backoffMs = BACKOFF_MIN_MS;

        while (true) {
            // Registered before the attempt, so a release that lands right after a refusal is not missed
            CompletableFuture<Void> released = new CompletableFuture<>();
            waiters.computeIfAbsent(key, k -> new CopyOnWriteArrayList<>()).add(released);
            try {
                UnknownOutcome unknown = null;
                Optional<ManagedLock> lock = Optional.empty();
                try {
                    lock = attempt(key, options.ttl());
                } catch (UnknownOutcome e) {
                    if (!e.worthRetrying) {
                        throw e;
                    }
                    unknown = e;
                }
                if (lock.isPresent()) {
                    return lock;
                }
                long remainingNanos = deadlineNanos - System.nanoTime();
                if (remainingNanos <= 0) {
                    if (unknown != null) {
                        throw unknown;
                    }
                    return Optional.empty();
                }
                if (unknown != null) {
                    log.debug("Attempt to acquire lock '{}' got no definite answer ({}). Trying again.",
                            key, unknown.reported.toString());
                }
                // Normally woken by the release event of this node. The timed wait is only a safety net, so the
                // number of Raft commands does not grow with the number of waiting threads.
                long jitteredMs = backoffMs / 2 + ThreadLocalRandom.current().nextLong(backoffMs / 2 + 1);
                awaitRelease(released, Math.min(remainingNanos, TimeUnit.MILLISECONDS.toNanos(jitteredMs)));
                backoffMs = Math.min(BACKOFF_MAX_MS, backoffMs * 2);
            } finally {
                removeWaiter(key, released);
            }
        }
    }

    private static void awaitRelease(CompletableFuture<Void> released, long nanos) {
        try {
            released.get(nanos, TimeUnit.NANOSECONDS);
        } catch (TimeoutException e) {
            // time for the next attempt
        } catch (ExecutionException e) {
            throw new IllegalStateException("A lock waiter can only be completed normally", e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new KurinLockAcquisitionException("Interrupted while waiting for a lock");
        }
    }

    private void removeWaiter(String key, CompletableFuture<Void> waiter) {
        waiters.computeIfPresent(key, (k, list) -> {
            list.remove(waiter);
            return list.isEmpty() ? null : list;
        });
    }

    // Called on the Raft thread of this node: only completes a future that another thread is parked on.
    // One waiter per release is woken, the longest waiting first: a freed lock can be taken by one thread only,
    // and waking all of them would cost one Raft command per waiter.
    private void onLockReleased(String key) {
        List<CompletableFuture<Void>> waiting = waiters.get(key);
        if (waiting != null) {
            for (CompletableFuture<Void> waiter : waiting) {
                if (waiter.complete(null)) {
                    return;
                }
            }
        }
    }

    /**
     * An attempt that the cluster did not answer definitely. It never leaves the manager: the caller gets
     * {@link #reported}.
     */
    private static final class UnknownOutcome extends RuntimeException {
        final KurinException reported;
        final boolean worthRetrying;

        UnknownOutcome(KurinException reported, boolean worthRetrying) {
            super(null, null, false, false);
            this.reported = reported;
            this.worthRetrying = worthRetrying;
        }
    }

    /**
     * @return empty only if the state machine answered that another owner holds the lock
     * @throws UnknownOutcome if nothing definite is known about the attempt
     */
    private Optional<ManagedLock> attempt(String key, Duration ttl) {
        // A fresh owner id per attempt: if the outcome stays unknown, this exact attempt can be released
        String ownerId = cluster.nodeId() + ":" + UUID.randomUUID();
        long requestedNanos = System.nanoTime();
        AcquireResult result;
        try {
            result = cluster.<AcquireResult>submitCommand(new AcquireLock(key, ownerId, ttl.toMillis()), commandTimeout)
                    .get(commandTimeout.toMillis() + 1000, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            releaseInBackground(key, ownerId, COMPENSATION_ATTEMPTS);
            throw new KurinLockAcquisitionException("Interrupted while acquiring lock '" + key + "'");
        } catch (ExecutionException | TimeoutException e) {
            Throwable cause = e instanceof ExecutionException ? e.getCause() : e;
            if (cause instanceof KurinNoLeaderException) {
                // The node knew no leader to give the command to: nothing was submitted, nothing is to be undone
                log.debug("Attempt to acquire lock '{}' was not submitted: {}", key, cause.toString());
            } else {
                log.debug("Attempt to acquire lock '{}' got no definite answer ({}). Releasing it in case it was granted.",
                        key, cause.toString());
                // The command may have been committed. The release names the owner id of this attempt, which
                // nobody else knows, so it can be repeated and can never remove the grant of another attempt.
                releaseInBackground(key, ownerId, COMPENSATION_ATTEMPTS);
            }
            throw unknownOutcome(key, cause);
        }
        if (!result.granted()) {
            return Optional.empty();
        }
        return Optional.of(new ManagedLock(key, ownerId, result.fencingToken(), result.expiresAtMs(),
                requestedNanos + ttl.toNanos()));
    }

    // Keeps the kind of the failure and adds which lock it was about; the original stays the cause
    private static UnknownOutcome unknownOutcome(String key, Throwable cause) {
        String what = "Lock '" + key + "' was not acquired, and it is not known to be busy: ";
        if (cause instanceof KurinNoLeaderException) {
            return new UnknownOutcome(new KurinNoLeaderException(what + cause.getMessage(), cause), true);
        }
        if (cause instanceof KurinRemoteException) {
            return new UnknownOutcome(new KurinRemoteException(what + cause.getMessage(), cause), true);
        }
        if (cause instanceof KurinTimeoutException) {
            return new UnknownOutcome(new KurinTimeoutException(what + cause.getMessage(), cause), true);
        }
        if (cause instanceof TimeoutException) {
            return new UnknownOutcome(new KurinTimeoutException(what + "the node gave no result in time", cause), true);
        }
        if (cause instanceof KurinException known) {
            // The node is shut down, the command cannot be serialized, its handler failed: waiting changes nothing
            boolean permanent = known instanceof KurinNotStartedException || known instanceof KurinSerializationException
                    || known instanceof KurinCommandFailedException;
            return new UnknownOutcome(known, !permanent);
        }
        return new UnknownOutcome(new KurinException(what + cause, cause), false);
    }

    private void releaseInBackground(String key, String ownerId, int attemptsLeft) {
        cluster.<Boolean>submitCommand(new ReleaseLock(key, ownerId), commandTimeout).whenComplete((released, error) -> {
            if (error == null) {
                return;
            }
            if (attemptsLeft > 1 && cluster.isStarted()) {
                long delayMs = BACKOFF_MIN_MS << Math.min(COMPENSATION_ATTEMPTS - attemptsLeft, 4);
                try {
                    scheduler.schedule(() -> releaseInBackground(key, ownerId, attemptsLeft - 1), delayMs, TimeUnit.MILLISECONDS);
                    return;
                } catch (RejectedExecutionException closed) {
                    log.debug("The lock manager is closed: the release of lock '{}' is not retried", key);
                }
            }
            log.warn("Could not confirm the release of lock '{}' held by {}: {}. It expires with its time to live.",
                    key, ownerId, error.toString());
            listeners.forEach(listener -> listener.onReleaseUnconfirmed(key));
        });
    }

    @Override
    public void close() {
        scheduler.shutdownNow();
    }

    // ---------------------------------------------------------------- handles

    private final class ManagedLock implements LockHandle {
        private final String key;
        private final String ownerId;
        private final long fencingToken;
        private volatile long expiresAtMs;
        // Measured from before the request was sent, so the local view never outlives the real lease
        private volatile long localDeadlineNanos;
        private volatile boolean released;
        private volatile boolean lost;
        private volatile ScheduledFuture<?> renewal;

        ManagedLock(String key, String ownerId, long fencingToken, long expiresAtMs, long localDeadlineNanos) {
            this.key = key;
            this.ownerId = ownerId;
            this.fencingToken = fencingToken;
            this.expiresAtMs = expiresAtMs;
            this.localDeadlineNanos = localDeadlineNanos;
        }

        @Override
        public String key() {
            return key;
        }

        @Override
        public long fencingToken() {
            return fencingToken;
        }

        @Override
        public Instant expiresAt() {
            return Instant.ofEpochMilli(expiresAtMs);
        }

        @Override
        public boolean isHeld() {
            return !released && !lost && localDeadlineNanos - System.nanoTime() > 0;
        }

        @Override
        public boolean renew(Duration ttl) {
            if (released || lost) {
                return false;
            }
            long requestedNanos = System.nanoTime();
            try {
                RenewResult result = cluster.<RenewResult>submitCommand(new RenewLock(key, ownerId, ttl.toMillis()), commandTimeout)
                        .get(commandTimeout.toMillis() + 1000, TimeUnit.MILLISECONDS);
                if (!result.renewed()) {
                    markLost("the cluster no longer records this owner");
                    return false;
                }
                expiresAtMs = result.expiresAtMs();
                localDeadlineNanos = requestedNanos + ttl.toNanos();
                return true;
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return isHeld();
            } catch (ExecutionException | TimeoutException e) {
                // Unknown outcome: the lock may or may not be extended. The old deadline stays authoritative.
                log.debug("Renewal of lock '{}' got no definite answer: {}", key, e.toString());
                return false;
            }
        }

        void startAutoRenew(Duration ttl) {
            long periodMs = Math.max(1, ttl.toMillis() / 3);
            renewal = scheduler.scheduleWithFixedDelay(() -> {
                if (released || lost) {
                    stopAutoRenew();
                    return;
                }
                if (!renew(ttl) && !isHeld()) {
                    markLost("automatic renewal failed");
                }
            }, periodMs, periodMs, TimeUnit.MILLISECONDS);
        }

        private void stopAutoRenew() {
            ScheduledFuture<?> current = renewal;
            if (current != null) {
                current.cancel(false);
            }
        }

        private void markLost(String reason) {
            if (!lost && !released) {
                lost = true;
                stopAutoRenew();
                log.warn("Lock '{}' (token {}) is lost: {}", key, fencingToken, reason);
            }
        }

        @Override
        public void release() {
            if (released) {
                return;
            }
            released = true;
            stopAutoRenew();
            listeners.forEach(listener -> listener.onRelease(key));
            try {
                cluster.<Boolean>submitCommand(new ReleaseLock(key, ownerId), commandTimeout)
                        .get(commandTimeout.toMillis() + 1000, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                releaseInBackground(key, ownerId, COMPENSATION_ATTEMPTS);
            } catch (ExecutionException | TimeoutException e) {
                log.debug("Release of lock '{}' got no definite answer: {}. Retrying in the background.", key, e.toString());
                releaseInBackground(key, ownerId, COMPENSATION_ATTEMPTS);
            }
        }
    }

    /**
     * One level of a reentrant acquisition. Releasing it gives up the lock only at the outermost level.
     */
    private final class ReentrantView implements LockHandle {
        private final ReentrantHold hold;
        private final Thread owner = Thread.currentThread();
        private boolean released;

        ReentrantView(ReentrantHold hold) {
            this.hold = hold;
        }

        @Override
        public String key() {
            return hold.lock.key();
        }

        @Override
        public long fencingToken() {
            return hold.lock.fencingToken();
        }

        @Override
        public Instant expiresAt() {
            return hold.lock.expiresAt();
        }

        @Override
        public boolean renew(Duration ttl) {
            return !released && hold.lock.renew(ttl);
        }

        @Override
        public boolean isHeld() {
            return !released && hold.lock.isHeld();
        }

        @Override
        public void release() {
            if (released) {
                return;
            }
            if (Thread.currentThread() != owner) {
                throw new IllegalStateException("A reentrant lock must be released by the thread that acquired it");
            }
            released = true;
            if (--hold.count == 0) {
                Map<String, ReentrantHold> holds = reentrantHolds.get();
                holds.remove(hold.lock.key(), hold);
                if (holds.isEmpty()) {
                    reentrantHolds.remove();
                }
                hold.lock.release();
            }
        }
    }
}
