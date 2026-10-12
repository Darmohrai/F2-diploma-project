package org.kurin.kurintieredcache.tier;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Runs tasks asynchronously while keeping the order of tasks that share a key: each key maps to one of a
 * fixed number of single-threaded stripes.
 */
public class StripedExecutor implements AutoCloseable {
    private final ExecutorService[] stripes;

    public StripedExecutor(int stripeCount, String threadNamePrefix) {
        if (stripeCount <= 0) {
            throw new IllegalArgumentException("The number of stripes must be positive");
        }
        AtomicInteger threadNumber = new AtomicInteger();
        this.stripes = new ExecutorService[stripeCount];
        for (int i = 0; i < stripeCount; i++) {
            stripes[i] = Executors.newSingleThreadExecutor(task -> {
                Thread thread = new Thread(task, threadNamePrefix + "-" + threadNumber.incrementAndGet());
                thread.setDaemon(true);
                return thread;
            });
        }
    }

    /**
     * @return a future completed when the task ran; exceptionally if it threw or the executor is closed
     */
    public CompletableFuture<Void> execute(String key, Runnable task) {
        ExecutorService stripe = stripes[Math.floorMod(key.hashCode(), stripes.length)];
        try {
            return CompletableFuture.runAsync(task, stripe);
        } catch (RejectedExecutionException e) {
            return CompletableFuture.failedFuture(e);
        }
    }

    /**
     * Runs the task after everything submitted so far on every stripe.
     */
    public CompletableFuture<Void> executeOnAll(Runnable task) {
        CompletableFuture<?>[] barriers = new CompletableFuture<?>[stripes.length];
        try {
            // The task itself runs on stripe 0, once the others have drained what was queued before
            for (int i = 1; i < stripes.length; i++) {
                barriers[i] = CompletableFuture.runAsync(() -> {
                }, stripes[i]);
            }
            barriers[0] = CompletableFuture.completedFuture(null);
            return CompletableFuture.allOf(barriers).thenRunAsync(task, stripes[0]);
        } catch (RejectedExecutionException e) {
            return CompletableFuture.failedFuture(e);
        }
    }

    @Override
    public void close() {
        for (ExecutorService stripe : stripes) {
            stripe.shutdown();
        }
    }
}
