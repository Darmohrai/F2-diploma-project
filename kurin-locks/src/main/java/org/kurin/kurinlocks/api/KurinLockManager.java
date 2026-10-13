package org.kurin.kurinlocks.api;

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
 */
public class KurinLockManager implements AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(KurinLockManager.class);

    private static final long BACKOFF_MIN_MS = 50;
    private static final long BACKOFF_MAX_MS = 1000;
    private static final int COMPENSATION_ATTEMPTS = 5;

    private final KurinNode node;
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
        this.node = node;
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
     * @return the handle, or empty if another owner holds the lock or the cluster did not confirm the grant
     */
    public Optional<LockHandle> tryLock(String key, Duration ttl) {
        return tryLock(key, LockOptions.ttl(ttl));
    }

    /**
     * Tries to take the lock, waiting up to {@code waitTimeout} for the current holder to let go.
     */
    public Optional<LockHandle> tryLock(String key, Duration ttl, Duration waitTimeout) {
        return tryLock(key, LockOptions.ttl(ttl).waitTimeout(waitTimeout));
    }

    /**
     * @throws KurinLockAcquisitionException if the lock was not obtained within {@code waitTimeout}
     */
    public LockHandle lock(String key, Duration ttl, Duration waitTimeout) {
        return lock(key, LockOptions.ttl(ttl).waitTimeout(waitTimeout));
    }

    public LockHandle lock(String key, LockOptions options) {
        return tryLock(key, options).orElseThrow(() -> new KurinLockAcquisitionException(
                "Lock '" + key + "' was not acquired within " + options.waitTimeout().toMillis() + " ms"));
    }

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
        // The key is not a tag: it is unbounded, and tags must stay few
        org.kurin.api.KurinInstrumentation.Span span = node.startSpan("kurin.lock.acquire", Map.of());
        Optional<ManagedLock> acquired;
        try {
            acquired = acquireWithWait(key, options, startedNanos);
        } catch (RuntimeException e) {
            span.end(e);
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
                Optional<ManagedLock> lock = attempt(key, options.ttl());
                if (lock.isPresent()) {
                    return lock;
                }
                long remainingNanos = deadlineNanos - System.nanoTime();
                if (remainingNanos <= 0) {
                    return Optional.empty();
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

    private Optional<ManagedLock> attempt(String key, Duration ttl) {
        // A fresh owner id per attempt: if the outcome stays unknown, this exact attempt can be released
        String ownerId = node.nodeId() + ":" + UUID.randomUUID();
        long requestedNanos = System.nanoTime();
        try {
            AcquireResult result = node.<AcquireResult>submitCommand(new AcquireLock(key, ownerId, ttl.toMillis()), commandTimeout)
                    .get(commandTimeout.toMillis() + 1000, TimeUnit.MILLISECONDS);
            if (!result.granted()) {
                return Optional.empty();
            }
            return Optional.of(new ManagedLock(key, ownerId, result.fencingToken(), result.expiresAtMs(),
                    requestedNanos + ttl.toNanos()));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            releaseInBackground(key, ownerId, COMPENSATION_ATTEMPTS);
            throw new KurinLockAcquisitionException("Interrupted while acquiring lock '" + key + "'");
        } catch (ExecutionException | TimeoutException e) {
            Throwable cause = e instanceof ExecutionException ? e.getCause() : e;
            log.debug("Attempt to acquire lock '{}' got no definite answer ({}). Releasing it in case it was granted.",
                    key, cause.toString());
            // The command may have been committed. Its owner id is known to nobody else, so undoing it is safe.
            releaseInBackground(key, ownerId, COMPENSATION_ATTEMPTS);
            return Optional.empty();
        }
    }

    private void releaseInBackground(String key, String ownerId, int attemptsLeft) {
        node.<Boolean>submitCommand(new ReleaseLock(key, ownerId), commandTimeout).whenComplete((released, error) -> {
            if (error == null) {
                return;
            }
            if (attemptsLeft <= 1 || !node.isStarted()) {
                log.warn("Could not confirm the release of lock '{}' held by {}: {}. It expires with its time to live.",
                        key, ownerId, error.toString());
                return;
            }
            long delayMs = BACKOFF_MIN_MS << Math.min(COMPENSATION_ATTEMPTS - attemptsLeft, 4);
            scheduler.schedule(() -> releaseInBackground(key, ownerId, attemptsLeft - 1), delayMs, TimeUnit.MILLISECONDS);
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
                RenewResult result = node.<RenewResult>submitCommand(new RenewLock(key, ownerId, ttl.toMillis()), commandTimeout)
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
                node.<Boolean>submitCommand(new ReleaseLock(key, ownerId), commandTimeout)
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
