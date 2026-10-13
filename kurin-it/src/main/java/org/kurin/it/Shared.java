package org.kurin.it;

import org.kurin.test.InMemorySharedExternalStore;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * What the instances of one JVM have in common, standing in for what real instances share outside of
 * themselves: an external cache, and a place where the tests can see what each instance did.
 */
public final class Shared {
    /**
     * The external cache tier of all instances.
     */
    public static final InMemorySharedExternalStore EXTERNAL_CACHE = new InMemorySharedExternalStore();

    /**
     * One entry per run of the nightly job: the instance that ran it.
     */
    public static final List<String> JOB_RUNS = new CopyOnWriteArrayList<>();

    public static final AtomicInteger INSIDE_LOCK = new AtomicInteger();
    public static final AtomicInteger MOST_INSIDE_LOCK = new AtomicInteger();
    public static final AtomicInteger LOCKED_RUNS = new AtomicInteger();

    /**
     * Runs of the methods that show what each busy policy of a lock does.
     */
    public static final AtomicInteger POLICY_RUNS = new AtomicInteger();

    private Shared() {
    }

    public static void reset() {
        JOB_RUNS.clear();
        INSIDE_LOCK.set(0);
        MOST_INSIDE_LOCK.set(0);
        LOCKED_RUNS.set(0);
        POLICY_RUNS.set(0);
    }
}
