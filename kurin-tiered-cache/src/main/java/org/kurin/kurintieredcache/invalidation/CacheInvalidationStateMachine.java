package org.kurin.kurintieredcache.invalidation;

import org.kurin.api.KurinCommand;
import org.kurin.api.KurinRestore;
import org.kurin.api.KurinService;
import org.kurin.api.KurinSnapshot;
import org.kurin.kurintieredcache.core.KurinCacheManager;
import org.kurin.kurintieredcache.core.TieredCache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;

/**
 * Applies replicated cache invalidations to the tiers of this instance.
 * <p>
 * Its replicated state is a single number: how many invalidations were applied. Each instance also remembers,
 * next to its disk tier, up to which number its local tiers are clean. When an instance is brought up to date
 * by a snapshot instead of by the individual commands, the two numbers differ: it cannot know what it missed,
 * so it drops all of its local cache data.
 */
@KurinService("kurin.cache.invalidation")
public class CacheInvalidationStateMachine {
    private static final Logger log = LoggerFactory.getLogger(CacheInvalidationStateMachine.class);

    private final KurinCacheManager manager;
    private final List<Consumer<String>> appliedListeners = new CopyOnWriteArrayList<>();
    private long sequence;

    public CacheInvalidationStateMachine(KurinCacheManager manager) {
        this.manager = manager;
    }

    /**
     * Registers a listener told the id of every invalidation applied on this instance. It runs on the Raft
     * thread and must not block.
     */
    public void addAppliedListener(Consumer<String> listener) {
        appliedListeners.add(listener);
    }

    @KurinCommand
    public void evict(EvictCacheCommand command) {
        sequence++;
        try {
            TieredCache cache = manager.getCache(command.cacheName());
            if (command.allEntries()) {
                cache.clearLocal("replicated");
            } else {
                for (String key : command.keys()) {
                    cache.evictLocal(key, "replicated");
                }
            }
            manager.recordInvalidationSequence(sequence);
        } catch (RuntimeException e) {
            // The sequence is not recorded: after a restart this instance sees the gap and clears its tiers
            log.error("Invalidation of cache '{}' could not be applied on this instance", command.cacheName(), e);
        }
        for (Consumer<String> listener : appliedListeners) {
            listener.accept(command.invalidationId());
        }
    }

    @KurinSnapshot
    public byte[] snapshot() {
        return ByteBuffer.allocate(Long.BYTES).putLong(sequence).array();
    }

    @KurinRestore
    public void restore(byte[] data) {
        sequence = ByteBuffer.wrap(data).getLong();
        long local = manager.localInvalidationSequence();
        if (local != sequence) {
            log.info("This instance applied {} cache invalidations locally, the cluster is at {}: dropping local cache "
                    + "data, because the invalidations in between are not known", local, sequence);
            manager.clearAllLocal("resync");
            manager.recordInvalidationSequence(sequence);
        }
    }
}
