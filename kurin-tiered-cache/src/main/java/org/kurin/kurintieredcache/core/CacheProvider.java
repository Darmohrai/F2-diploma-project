package org.kurin.kurintieredcache.core;

import org.kurin.kurintieredcache.api.CacheTier;

public interface CacheProvider {

    boolean supports(CacheTier tier);

    KurinCache createCache(String name, CacheConfigurationContext context);
}
