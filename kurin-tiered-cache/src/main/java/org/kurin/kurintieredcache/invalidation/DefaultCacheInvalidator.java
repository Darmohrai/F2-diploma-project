package org.kurin.kurintieredcache.invalidation;

import org.kurin.api.exception.KurinTimeoutException;
import org.kurin.kurintieredcache.api.KurinCacheInvalidator;
import org.kurin.kurintieredcache.core.KurinCacheManager;
import org.kurin.kurintieredcache.core.TieredCache;
import org.kurin.network.dto.RpcResult;
import org.kurin.raft.KurinNode;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/**
 * Invalidates on this instance and in the shared tier at once, then on all instances through Raft.
 */
public class DefaultCacheInvalidator implements KurinCacheInvalidator {
    private final KurinNode node;
    private final KurinCacheManager manager;
    private final Duration timeout;
    private final Map<String, CompletableFuture<Void>> awaitingLocalApply = new ConcurrentHashMap<>();

    public DefaultCacheInvalidator(KurinNode node, KurinCacheManager manager, CacheInvalidationStateMachine stateMachine,
                                   Duration timeout) {
        this.node = node;
        this.manager = manager;
        this.timeout = timeout;
        stateMachine.addAppliedListener(invalidationId -> {
            CompletableFuture<Void> waiting = awaitingLocalApply.remove(invalidationId);
            if (waiting != null) {
                // Completed off the Raft thread: callbacks of the application must not run there
                waiting.completeAsync(() -> null);
            }
        });
    }

    @Override
    public CompletableFuture<Void> evict(String cacheName, String... keys) {
        if (keys.length == 0) {
            return CompletableFuture.completedFuture(null);
        }
        TieredCache cache = manager.getCache(cacheName);
        CompletableFuture<?>[] shared = new CompletableFuture<?>[keys.length];
        for (int i = 0; i < keys.length; i++) {
            shared[i] = cache.evictShared(keys[i]);
            // This instance stops serving the entry immediately, whatever happens to the replication
            cache.evictLocal(keys[i], "local");
        }
        return replicate(CompletableFuture.allOf(shared), cacheName, List.of(keys), false);
    }

    @Override
    public CompletableFuture<Void> evictAll(String cacheName) {
        TieredCache cache = manager.getCache(cacheName);
        CompletableFuture<Void> sharedCleared = cache.clearShared();
        cache.clearLocal("local");
        return replicate(sharedCleared, cacheName, List.of(), true);
    }

    // Order matters: the shared tier is clean before the other instances are told to drop their copies,
    // so none of them can refill itself from a stale shared entry
    private CompletableFuture<Void> replicate(CompletableFuture<Void> sharedCleaned, String cacheName, List<String> keys,
                                              boolean allEntries) {
        String invalidationId = UUID.randomUUID().toString();
        CompletableFuture<Void> applied = new CompletableFuture<>();
        awaitingLocalApply.put(invalidationId, applied);

        sharedCleaned
                .thenCompose(cleaned -> node.<Void>submitCommand(
                        new EvictCacheCommand(invalidationId, cacheName, keys, allEntries, node.nodeId()), timeout))
                .whenComplete((committed, error) -> {
                    if (error != null && awaitingLocalApply.remove(invalidationId) != null) {
                        applied.completeExceptionally(RpcResult.unwrap(error));
                    }
                });

        // Committed on the leader, but this instance may be a follower that is cut off and never applies it
        return applied.orTimeout(timeout.toMillis() * 2, TimeUnit.MILLISECONDS)
                .exceptionallyCompose(error -> {
                    awaitingLocalApply.remove(invalidationId);
                    Throwable cause = RpcResult.unwrap(error);
                    return CompletableFuture.failedFuture(cause instanceof java.util.concurrent.TimeoutException
                            ? new KurinTimeoutException("Invalidation of cache '" + cacheName
                            + "' was not applied on this instance within " + timeout.toMillis() * 2 + " ms")
                            : cause);
                });
    }
}
