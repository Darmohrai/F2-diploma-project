package org.kurin.kurintieredcache.core;

import org.kurin.network.serializer.KryoSerializer;

public record CacheConfigurationContext(
        KryoSerializer serializer,
        String storageBaseDir
) {
}
