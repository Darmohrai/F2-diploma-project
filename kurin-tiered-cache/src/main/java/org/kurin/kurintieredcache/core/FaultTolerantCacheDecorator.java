package org.kurin.kurintieredcache.core;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicInteger;

public class FaultTolerantCacheDecorator implements KurinCache {
    private static final Logger log = LoggerFactory.getLogger(FaultTolerantCacheDecorator.class);

    private final KurinCache delegate;
    private final String name;
    private final int maxFailures;
    private final long openTimeoutMs;

    private final AtomicInteger failureCount = new AtomicInteger(0);
    private volatile long circuitOpenUntil = 0;

    public FaultTolerantCacheDecorator(KurinCache delegate, int maxFailures, long openTimeoutMs) {
        this.delegate = delegate;
        this.name = delegate.getName();
        this.maxFailures = maxFailures;
        this.openTimeoutMs = openTimeoutMs;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public Object get(String key) {
        if (!isCircuitClosed()) return null;
        try {
            Object value = delegate.get(key);
            onSuccess();
            return value;
        } catch (Exception e) {
            onFailure("GET", e);
            return null;
        }
    }

    @Override
    public void put(String key, Object value) {
        if (!isCircuitClosed()) return;
        try {
            delegate.put(key, value);
            onSuccess();
        } catch (Exception e) {
            onFailure("PUT", e);
        }
    }

    @Override
    public void evict(String key) {
        if (!isCircuitClosed()) return;
        try {
            delegate.evict(key);
            onSuccess();
        } catch (Exception e) {
            onFailure("EVICT", e);
        }
    }

    @Override
    public void clear() {
        if (!isCircuitClosed()) return;
        try {
            delegate.clear();
            onSuccess();
        } catch (Exception e) {
            onFailure("CLEAR", e);
        }
    }

    @Override
    public void close() {
        try {
            delegate.close();
        } catch (Exception e) {
            log.error("[L3 Cache] Error closing cache '{}': {}", name, e.getMessage());
        }
    }

    private boolean isCircuitClosed() {
        if (circuitOpenUntil == 0) return true;
        if (System.currentTimeMillis() > circuitOpenUntil) {
            circuitOpenUntil = 0;
            return true;
        }
        return false;
    }

    private void onSuccess() {
        failureCount.set(0);
        circuitOpenUntil = 0;
    }

    private void onFailure(String operation, Exception e) {
        int failures = failureCount.incrementAndGet();
        log.warn("[L3 Cache] Operation {} failed in cache '{}': {}", operation, name, e.getMessage());
        if (failures >= maxFailures && circuitOpenUntil == 0) {
            log.error("[L3 Cache] Circuit Breaker OPENED for '{}'. Pausing requests for {} ms.", name, openTimeoutMs);
            circuitOpenUntil = System.currentTimeMillis() + openTimeoutMs;
        }
    }
}
