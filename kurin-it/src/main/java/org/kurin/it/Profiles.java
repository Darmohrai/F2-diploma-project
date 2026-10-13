package org.kurin.it;

import org.kurin.kurintieredcache.api.KurinCacheEvict;
import org.kurin.kurintieredcache.api.KurinCacheable;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * A cached view of the accounts. The cache of every instance is dropped when any instance changes an account.
 */
@Service
public class Profiles {
    private final Accounts accounts;
    private final AtomicInteger loads = new AtomicInteger();

    public Profiles(Accounts accounts) {
        this.accounts = accounts;
    }

    @KurinCacheable(cacheName = "profiles", key = "#id")
    public String profile(String id) {
        loads.incrementAndGet();
        return "profile of " + accounts.currentNameOf(id);
    }

    @Transactional
    @KurinCacheEvict(cacheNames = "profiles", key = "#id")
    public void rename(@org.kurin.routing.KurinShardKey String id, String name) {
        accounts.save(id, name);
    }

    public int loads() {
        return loads.get();
    }
}
