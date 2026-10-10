package org.kurin.kurintieredcache.core;

import org.kurin.network.serializer.KryoSerializer;

public record CacheConfigurationContext(
        KryoSerializer serializer,
        String storageBaseDir,
        long l1MaxSize,
        long l1ExpireAfterAccessMinutes,
        int l3MaxFailures,
        long l3CircuitOpenTimeoutMs
) {
}
