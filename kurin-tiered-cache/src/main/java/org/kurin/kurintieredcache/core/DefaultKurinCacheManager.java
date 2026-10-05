package org.kurin.kurintieredcache.core;

import org.kurin.kurintieredcache.api.CacheTier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class DefaultKurinCacheManager implements KurinCacheManager, AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(DefaultKurinCacheManager.class);

    private final Map<String, KurinCache> activeCaches = new ConcurrentHashMap<>();
    private final List<CacheProvider> providers;
    private final CacheConfigurationContext context;

    public DefaultKurinCacheManager(List<CacheProvider> providers, CacheConfigurationContext context) {
        this.providers = providers;
        this.context = context;
    }

    @Override
    public KurinCache getOrCreateCache(String cacheName, CacheTier tier) {
        return activeCaches.computeIfAbsent(cacheName, name -> {
            log.info("Creating new cache '{}' with tier {}", name, tier);

            if (tier == CacheTier.TIERED_SMART) {
                List<KurinCache> tiers = new ArrayList<>();
                tiers.add(resolveProvider(CacheTier.L1_MEMORY).createCache(name + "_L1", context));
                tiers.add(resolveProvider(CacheTier.L2_ROCKSDB).createCache(name + "_L2", context));

                CacheProvider l3Provider = findProvider(CacheTier.L3_EXTERNAL);
                if (l3Provider != null) {
                    KurinCache rawL3Cache = l3Provider.createCache(name + "_L3", context);
                    tiers.add(new FaultTolerantCacheDecorator(rawL3Cache));
                    log.info("L3 External Cache successfully integrated into composite cache '{}'", name);
                }

                return new TieredCompositeCache(name, tiers);
            }
            return resolveProvider(tier).createCache(name, context);
        });
    }

    @Override
    public KurinCache getCache(String cacheName) {
        return activeCaches.get(cacheName);
    }

    private CacheProvider resolveProvider(CacheTier targetTier) {
        CacheProvider provider = findProvider(targetTier);
        if (provider == null) {
            throw new IllegalStateException("No CacheProvider configured for tier: " + targetTier);
        }
        return provider;
    }

    private CacheProvider findProvider(CacheTier targetTier) {
        for (CacheProvider provider : providers) {
            if (provider.supports(targetTier)) {
                return provider;
            }
        }
        return null;
    }

    @Override
    public void close() {
        log.info("Shutting down KurinCacheManager. Closing {} active caches.", activeCaches.size());
        for (KurinCache cache : activeCaches.values()) {
            try {
                cache.close();
            } catch (Exception e) {
                log.error("Error closing cache: {}", cache.getName(), e);
            }
        }
        activeCaches.clear();
    }
}
