package org.kurin.kurintieredcache.core;

import org.kurin.kurintieredcache.api.CacheResult;
import org.kurin.kurintieredcache.api.KurinCacheObserver;
import org.kurin.kurintieredcache.api.KurinCacheTrace;
import org.kurin.kurintieredcache.api.StampedeMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;

/**
 * Fans cache events out to the registered observers and to the per-thread trace, shielding the cache from
 * observers that throw.
 */
public final class ObserverHub implements KurinCacheObserver {
    private static final Logger log = LoggerFactory.getLogger(ObserverHub.class);

    private final List<KurinCacheObserver> observers = new CopyOnWriteArrayList<>();

    public void add(KurinCacheObserver observer) {
        observers.add(observer);
    }

    private void each(Consumer<KurinCacheObserver> call) {
        for (KurinCacheObserver observer : observers) {
            try {
                call.accept(observer);
            } catch (RuntimeException e) {
                log.warn("Cache observer {} failed", observer.getClass().getName(), e);
            }
        }
    }

    @Override
    public void onLookup(String cache, String key, CacheResult result) {
        KurinCacheTrace.record(cache, key, result);
        each(observer -> observer.onLookup(cache, key, result));
    }

    @Override
    public void onLoad(String cache, String key, Duration duration, boolean success) {
        each(observer -> observer.onLoad(cache, key, duration, success));
    }

    @Override
    public void onEvict(String cache, String key, String reason) {
        each(observer -> observer.onEvict(cache, key, reason));
    }

    @Override
    public void onStampedeWait(String cache, String key, StampedeMode mode, Duration waited) {
        each(observer -> observer.onStampedeWait(cache, key, mode, waited));
    }

    @Override
    public void onExternalStoreCircuit(String cache, String state) {
        each(observer -> observer.onExternalStoreCircuit(cache, state));
    }
}
