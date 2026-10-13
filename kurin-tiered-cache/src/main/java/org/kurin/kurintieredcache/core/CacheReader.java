package org.kurin.kurintieredcache.core;

import org.kurin.kurintieredcache.api.CacheClusterLock;
import org.kurin.kurintieredcache.api.CacheResult;
import org.kurin.kurintieredcache.api.KurinCacheLockTimeoutException;
import org.kurin.kurintieredcache.api.OnLockTimeout;
import org.kurin.kurintieredcache.api.StampedeMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;

/**
 * Read-through: returns the cached value of a key or loads it from the source and caches it, with protection
 * against stampedes, serving of stale values during a refresh, and against writing back a value that was
 * invalidated while it was being loaded.
 */
public class CacheReader implements AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(CacheReader.class);
    private static final Duration CLUSTER_LOCK_SLACK = Duration.ofSeconds(30);

    /**
     * @param settings      lifetime of the entries this read creates
     * @param cacheNulls    whether a null result is cached
     * @param unless        tested on the loaded value; true means "return it but do not cache it". May be null.
     * @param lockWait      how long a CLUSTER read waits for the cluster lock
     */
    public record ReadOptions(CacheSettings settings, boolean cacheNulls, StampedeMode stampede, Duration lockWait,
                              OnLockTimeout onLockTimeout, Predicate<Object> unless) {
    }

    private final KurinCacheManager manager;
    private final CacheClusterLock clusterLock;
    private final ObserverHub observers;
    private final InvalidationEpochs epochs;
    private final ConcurrentHashMap<String, CompletableFuture<Object>> flights = new ConcurrentHashMap<>();
    private final ExecutorService refreshExecutor;
    private final AtomicBoolean missingClusterLockReported = new AtomicBoolean();

    /**
     * @param clusterLock lock for {@link StampedeMode#CLUSTER}; null if no cluster lock is available, in which
     *                    case CLUSTER reads behave like LOCAL ones
     */
    public CacheReader(KurinCacheManager manager, CacheClusterLock clusterLock) {
        this.manager = manager;
        this.clusterLock = clusterLock;
        this.observers = manager.observers();
        this.epochs = manager.epochs();
        AtomicInteger threadNumber = new AtomicInteger();
        this.refreshExecutor = Executors.newCachedThreadPool(task -> {
            Thread thread = new Thread(task, "kurin-cache-refresh-" + threadNumber.incrementAndGet());
            thread.setDaemon(true);
            return thread;
        });
    }

    /**
     * @param loader asks the source of truth for the value; may return null
     * @return the cached or loaded value
     * @throws Exception whatever the loader throws; failures of the cache itself never surface here
     */
    public Object read(TieredCache cache, String key, ReadOptions options, Callable<Object> loader) throws Exception {
        return read(cache, key, options, loader, result -> {
        });
    }

    /**
     * @param onLookup told how the cache answered before a possible load: a hit in some tier, a stale value or a miss
     */
    public Object read(TieredCache cache, String key, ReadOptions options, Callable<Object> loader,
                       java.util.function.Consumer<CacheResult> onLookup) throws Exception {
        long now = System.currentTimeMillis();
        CacheLookup lookup = cache.get(key);

        if (lookup.isHit() && lookup.entry().isFresh(now)) {
            observers.onLookup(cache.name(), key, CacheResult.hit(lookup.tier()));
            onLookup.accept(CacheResult.hit(lookup.tier()));
            return lookup.entry().value();
        }
        if (lookup.isHit() && lookup.entry().isStale(now)) {
            onLookup.accept(CacheResult.STALE);
            // Serve what we have and let one caller refresh it; a failed refresh leaves the stale value in place
            observers.onLookup(cache.name(), key, CacheResult.STALE);
            refreshInBackground(cache, key, options, loader);
            return lookup.entry().value();
        }

        observers.onLookup(cache.name(), key, CacheResult.MISS);
        onLookup.accept(CacheResult.MISS);
        return switch (effectiveMode(options.stampede())) {
            case NONE -> loadAndStore(cache, key, options, loader, false);
            case LOCAL -> singleFlight(cache, key, StampedeMode.LOCAL, () -> loadAndStore(cache, key, options, loader, false));
            case CLUSTER -> singleFlight(cache, key, StampedeMode.CLUSTER, () -> loadUnderClusterLock(cache, key, options, loader));
        };
    }

    private StampedeMode effectiveMode(StampedeMode requested) {
        if (requested == StampedeMode.CLUSTER && clusterLock == null) {
            if (missingClusterLockReported.compareAndSet(false, true)) {
                log.warn("A cache asks for cluster-wide stampede protection but no cluster lock is available "
                        + "(the Kurin locks module is absent). Falling back to per-instance protection.");
            }
            return StampedeMode.LOCAL;
        }
        return requested;
    }

    // ---------------------------------------------------------------- single flight

    private static String flightKey(TieredCache cache, String key) {
        return cache.name() + '\u0000' + key;
    }

    /**
     * Runs the action unless another thread of this instance is already running one for the same key, in which
     * case its outcome, value or exception, is shared.
     */
    private Object singleFlight(TieredCache cache, String key, StampedeMode mode, Callable<Object> action) throws Exception {
        String flightKey = flightKey(cache, key);
        CompletableFuture<Object> mine = new CompletableFuture<>();
        CompletableFuture<Object> running = flights.putIfAbsent(flightKey, mine);

        if (running != null) {
            long startedNanos = System.nanoTime();
            try {
                return running.get();
            } catch (ExecutionException e) {
                throw rethrowable(e.getCause());
            } finally {
                observers.onStampedeWait(cache.name(), key, mode, Duration.ofNanos(System.nanoTime() - startedNanos));
            }
        }

        try {
            Object value = action.call();
            mine.complete(value);
            return value;
        } catch (Throwable t) {
            mine.completeExceptionally(t);
            throw rethrowable(t);
        } finally {
            flights.remove(flightKey, mine);
        }
    }

    private static Exception rethrowable(Throwable t) {
        if (t instanceof Error error) {
            throw error;
        }
        return (Exception) t;
    }

    private void refreshInBackground(TieredCache cache, String key, ReadOptions options, Callable<Object> loader) {
        if (flights.containsKey(flightKey(cache, key))) {
            return; // a refresh of this key is already under way
        }
        refreshExecutor.execute(() -> {
            try {
                singleFlight(cache, key, StampedeMode.LOCAL, () -> loadAndStore(cache, key, options, loader, false));
            } catch (Exception e) {
                log.debug("Background refresh of '{}' in cache '{}' failed, the stale value stays: {}", key, cache.name(), e.toString());
            }
        });
    }

    // ---------------------------------------------------------------- cluster lock

    private Object loadUnderClusterLock(TieredCache cache, String key, ReadOptions options, Callable<Object> loader) throws Exception {
        String lockKey = "kurin:cache:" + cache.name() + ":" + key;
        Optional<CacheClusterLock.Held> lock;
        try {
            // Long enough for a slow load; the lock is released explicitly as soon as the value is stored
            lock = clusterLock.tryLock(lockKey, options.lockWait().plus(CLUSTER_LOCK_SLACK), options.lockWait());
        } catch (RuntimeException e) {
            log.warn("Cluster lock for '{}' in cache '{}' failed: {}. Loading without it.", key, cache.name(), e.toString());
            return loadAndStore(cache, key, options, loader, false);
        }

        if (lock.isEmpty()) {
            return switch (options.onLockTimeout()) {
                case LOAD -> loadAndStore(cache, key, options, loader, false);
                case FAIL -> throw new KurinCacheLockTimeoutException("No cluster lock for '" + key + "' in cache '"
                        + cache.name() + "' within " + options.lockWait().toMillis() + " ms");
                case STALE -> {
                    CacheLookup stored = cache.get(key);
                    if (stored.isHit()) {
                        yield stored.entry().value();
                    }
                    throw new KurinCacheLockTimeoutException("No cluster lock for '" + key + "' in cache '" + cache.name()
                            + "' within " + options.lockWait().toMillis() + " ms, and no stale value to return");
                }
            };
        }

        try (CacheClusterLock.Held held = lock.get()) {
            // Whoever held the lock before us may have loaded the value: look again, shared tier included
            CacheLookup again = cache.get(key);
            if (again.isHit() && again.entry().isFresh(System.currentTimeMillis())) {
                observers.onLookup(cache.name(), key, CacheResult.hit(again.tier()));
                return again.entry().value();
            }
            // The value must be visible in the shared tier before the lock is released to the next instance
            return loadAndStore(cache, key, options, loader, true);
        }
    }

    // ---------------------------------------------------------------- loading

    private Object loadAndStore(TieredCache cache, String key, ReadOptions options, Callable<Object> loader,
                                boolean awaitSharedTier) throws Exception {
        InvalidationEpochs.Token token = epochs.capture(cache.name(), key);
        long startedNanos = System.nanoTime();
        Object value;
        try {
            value = loader.call();
        } catch (Throwable t) {
            observers.onLoad(cache.name(), key, Duration.ofNanos(System.nanoTime() - startedNanos), false);
            throw rethrowable(t);
        }
        observers.onLoad(cache.name(), key, Duration.ofNanos(System.nanoTime() - startedNanos), true);

        if (value == null && !options.cacheNulls()) {
            return null;
        }
        if (options.unless() != null && options.unless().test(value)) {
            return value;
        }
        CacheValueGuard.check(cache.name(), value);

        if (!epochs.isCurrent(cache.name(), key, token)) {
            log.debug("'{}' in cache '{}' was invalidated while it was loading: the loaded value is not cached", key, cache.name());
            return value;
        }
        CacheEntry entry = options.settings().newEntry(value, System.currentTimeMillis());
        CompletableFuture<Void> flushed = cache.putAndFlush(key, entry);
        if (!epochs.isCurrent(cache.name(), key, token)) {
            // The invalidation slipped in between the check and the write: take the value out again
            cache.evictLocal(key, "raced");
            return value;
        }
        if (awaitSharedTier) {
            awaitQuietly(flushed, manager.externalTimeout().multipliedBy(2));
        }
        return value;
    }

    private static void awaitQuietly(CompletableFuture<Void> future, Duration timeout) {
        try {
            future.get(Math.max(1, timeout.toMillis()), TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (ExecutionException | TimeoutException e) {
            log.debug("The shared tier was not confirmed in time: {}", e.toString());
        }
    }

    @Override
    public void close() {
        refreshExecutor.shutdownNow();
    }
}
