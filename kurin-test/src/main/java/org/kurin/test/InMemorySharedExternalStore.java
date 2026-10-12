package org.kurin.test;

import org.kurin.kurintieredcache.api.ExternalCacheStore;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * An {@link ExternalCacheStore} for tests: an in-memory map that several application instances in one JVM can
 * share, standing in for Redis or a similar store. It honours time to live, records every operation in order
 * and can be told to hang or to fail, to exercise timeouts and the circuit breaker of the cache.
 */
public class InMemorySharedExternalStore implements ExternalCacheStore {

    private record Stored(byte[] value, long expiresAtNanos) {
        boolean isExpired() {
            return expiresAtNanos != 0 && System.nanoTime() - expiresAtNanos >= 0;
        }
    }

    private final Map<String, Stored> entries = new ConcurrentHashMap<>();
    private final List<String> operations = new CopyOnWriteArrayList<>();
    private final AtomicInteger calls = new AtomicInteger();
    private final boolean shared;
    private volatile CountDownLatch stall;
    private volatile long stallMillis;
    private volatile RuntimeException failure;

    public InMemorySharedExternalStore() {
        this(true);
    }

    /**
     * @param shared what {@link #isShared()} reports
     */
    public InMemorySharedExternalStore(boolean shared) {
        this.shared = shared;
    }

    @Override
    public byte[] get(String key) {
        enter("get:" + key);
        Stored stored = entries.get(key);
        if (stored == null) {
            return null;
        }
        if (stored.isExpired()) {
            entries.remove(key, stored);
            return null;
        }
        return stored.value();
    }

    @Override
    public void put(String key, byte[] value, Duration ttl) {
        enter("put:" + key);
        entries.put(key, new Stored(value, ttl != null ? System.nanoTime() + ttl.toNanos() : 0));
    }

    @Override
    public void delete(String key) {
        enter("delete:" + key);
        entries.remove(key);
    }

    @Override
    public void deleteByPrefix(String prefix) {
        enter("deleteByPrefix:" + prefix);
        entries.keySet().removeIf(key -> key.startsWith(prefix));
    }

    @Override
    public boolean isShared() {
        return shared;
    }

    private void enter(String operation) {
        calls.incrementAndGet();
        operations.add(operation);
        CountDownLatch latch = stall;
        if (latch != null) {
            try {
                // Released early by resume(); otherwise the call hangs for the configured time
                latch.await(stallMillis, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        RuntimeException error = failure;
        if (error != null) {
            throw error;
        }
    }

    // ---------------------------------------------------------------- test controls

    /**
     * Makes every following call hang for up to the given time, like a store that stopped answering.
     */
    public void stall(Duration duration) {
        stallMillis = duration.toMillis();
        stall = new CountDownLatch(1);
    }

    /**
     * Makes every following call throw the given exception.
     */
    public void fail(RuntimeException error) {
        failure = error;
    }

    /**
     * Ends {@link #stall} and {@link #fail}; calls that are hanging return at once.
     */
    public void resume() {
        failure = null;
        CountDownLatch latch = stall;
        stall = null;
        if (latch != null) {
            latch.countDown();
        }
    }

    /**
     * @return every call made so far, as {@code operation:key}, in the order the store saw them
     */
    public List<String> operations() {
        return List.copyOf(operations);
    }

    public void clearOperations() {
        operations.clear();
    }

    /**
     * @return the number of calls made so far, including those that hung or failed
     */
    public int calls() {
        return calls.get();
    }

    public boolean contains(String key) {
        Stored stored = entries.get(key);
        return stored != null && !stored.isExpired();
    }

    /**
     * @return the number of live entries
     */
    public int size() {
        return (int) entries.values().stream().filter(stored -> !stored.isExpired()).count();
    }

    /**
     * Stores raw bytes directly, bypassing the cache: for tests of how the cache treats foreign content.
     */
    public void putRaw(String key, byte[] value) {
        entries.put(key, new Stored(value, 0));
    }
}
