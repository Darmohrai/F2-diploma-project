package org.kurin.kurintieredcache.tier;

import org.kurin.kurintieredcache.api.CacheTier;
import org.kurin.kurintieredcache.api.ExternalCacheStore;
import org.kurin.kurintieredcache.api.KurinCacheValueCodec;
import org.kurin.kurintieredcache.core.CacheEntry;
import org.kurin.kurintieredcache.core.CacheLookup;
import org.kurin.kurintieredcache.core.KurinCache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;

/**
 * The external tier of one cache, on top of the {@link ExternalCacheStore} of the application.
 * <p>
 * The store is treated as unreliable. Every call has a timeout and goes through a circuit breaker, and no
 * failure reaches the caller: a failed read is a miss, a failed write is logged. Reads wait for the store
 * (at most the timeout); writes and deletes are queued, in order per key.
 */
public class ExternalTier implements KurinCache {
    private static final Logger log = LoggerFactory.getLogger(ExternalTier.class);

    /**
     * @param ioExecutor runs the calls into the store, so that a call that hangs can be abandoned after the timeout
     * @param writes     keeps writes and deletes of one key in the order they were issued
     */
    public record Resources(ExecutorService ioExecutor, StripedExecutor writes) {
    }

    private final String name;
    private final String keyPrefix;
    private final ExternalCacheStore store;
    private final KurinCacheValueCodec codec;
    private final Resources resources;
    private final long timeoutMs;
    private final CircuitBreaker breaker;

    public ExternalTier(String name, ExternalCacheStore store, KurinCacheValueCodec codec, Resources resources,
                        Duration timeout, int failureThreshold, Duration openDuration, Consumer<CircuitBreaker.State> onCircuitChange) {
        this.name = name;
        this.keyPrefix = "kurin:" + name + ":";
        this.store = store;
        this.codec = codec;
        this.resources = resources;
        this.timeoutMs = Math.max(1, timeout.toMillis());
        this.breaker = new CircuitBreaker(failureThreshold, openDuration.toNanos(), state -> {
            log.warn("Circuit of the external tier of cache '{}' is now {}", name, state);
            onCircuitChange.accept(state);
        });
    }

    @Override
    public String name() {
        return name;
    }

    public boolean isShared() {
        return store.isShared();
    }

    public CircuitBreaker.State circuitState() {
        return breaker.state();
    }

    @Override
    public CacheLookup get(String key) {
        byte[] bytes = call("get", key, () -> store.get(keyPrefix + key));
        if (bytes == null) {
            return CacheLookup.miss();
        }
        try {
            CacheEntry entry = codec.decode(bytes);
            return entry.isDead(System.currentTimeMillis()) ? CacheLookup.miss() : CacheLookup.hit(entry, CacheTier.L3);
        } catch (RuntimeException e) {
            log.warn("Ignoring an undecodable entry '{}' of cache '{}' in the external store: {}", key, name, e.getMessage());
            return CacheLookup.miss();
        }
    }

    /**
     * Queues the write behind earlier operations on the same key.
     */
    @Override
    public void put(String key, CacheEntry entry) {
        putAsync(key, entry);
    }

    /**
     * @return a future completed when the write was attempted, successfully or not; it never fails
     */
    public CompletableFuture<Void> putAsync(String key, CacheEntry entry) {
        byte[] bytes;
        try {
            bytes = codec.encode(entry);
        } catch (RuntimeException e) {
            log.warn("Value of '{}' in cache '{}' cannot be stored in the external store: {}", key, name, e.getMessage());
            return CompletableFuture.completedFuture(null);
        }
        long deadAtMs = entry.deadAtMs();
        return resources.writes().execute(key, () -> {
            Duration ttl = null;
            if (deadAtMs != 0) {
                long remainingMs = deadAtMs - System.currentTimeMillis();
                if (remainingMs <= 0) {
                    return; // expired while it waited in the queue
                }
                ttl = Duration.ofMillis(remainingMs);
            }
            Duration storeTtl = ttl;
            call("put", key, () -> {
                store.put(keyPrefix + key, bytes, storeTtl);
                return null;
            });
        }).exceptionally(error -> null);
    }

    @Override
    public void evict(String key) {
        evictAsync(key);
    }

    public CompletableFuture<Void> evictAsync(String key) {
        return resources.writes().execute(key, () -> call("delete", key, () -> {
            store.delete(keyPrefix + key);
            return null;
        })).exceptionally(error -> null);
    }

    @Override
    public void clear() {
        clearAsync();
    }

    public CompletableFuture<Void> clearAsync() {
        // Runs after every write queued so far, so an older put cannot land after the clear and resurrect an entry
        return resources.writes().executeOnAll(() -> call("deleteByPrefix", null, () -> {
            store.deleteByPrefix(keyPrefix);
            return null;
        })).exceptionally(error -> null);
    }

    /**
     * Makes one call into the store with the timeout and the circuit breaker.
     *
     * @return the result, or null if the call was refused, failed or timed out
     */
    private <T> T call(String operation, String key, Callable<T> action) {
        if (!breaker.tryAcquire()) {
            return null;
        }
        CompletableFuture<T> result = new CompletableFuture<>();
        try {
            resources.ioExecutor().execute(() -> {
                try {
                    result.complete(action.call());
                } catch (Throwable t) {
                    result.completeExceptionally(t);
                }
            });
            T value = result.get(timeoutMs, TimeUnit.MILLISECONDS);
            breaker.onSuccess();
            return value;
        } catch (TimeoutException e) {
            // A slow store counts as a failing one; the abandoned call finishes on its own thread
            breaker.onFailure();
            log.debug("External store call {}({}) of cache '{}' timed out after {} ms", operation, key, name, timeoutMs);
        } catch (ExecutionException e) {
            breaker.onFailure();
            log.debug("External store call {}({}) of cache '{}' failed: {}", operation, key, name, e.getCause().toString());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            breaker.onFailure();
        } catch (RuntimeException e) {
            breaker.onFailure();
            log.debug("External store call {}({}) of cache '{}' could not be started: {}", operation, key, name, e.toString());
        }
        return null;
    }

    @Override
    public void close() {
        // The store and the executors belong to the cache manager
    }
}
