package org.kurin.routing.shard;

import org.kurin.routing.KurinReadOnly;
import org.kurin.routing.KurinRoutingContext;
import org.kurin.routing.KurinRoutingException;
import org.kurin.routing.consistency.ConsistencyToken;
import org.kurin.routing.consistency.KurinConsistency;

import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Runs code against one shard, the shard of a key, or all shards.
 * <p>
 * The callback uses the ordinary data access of the application (a {@code JdbcTemplate}, repositories): while
 * it runs, the routing context of its thread points to the shard. For a fan-out every shard gets its own
 * thread from a bounded pool; the context is set inside each task and removed when the task ends. There is no
 * transaction across shards: each callback commits on its own.
 */
public class KurinShardOperations implements AutoCloseable {

    @FunctionalInterface
    public interface ShardCallback<T> {
        /**
         * @param shard the shard the calling thread is routed to
         */
        T execute(String shard) throws Exception;
    }

    public enum FailurePolicy {
        /**
         * The first failure cancels the shards still running and is thrown.
         */
        FAIL_FAST,
        /**
         * Every shard runs to its end; failures are reported per shard in the result.
         */
        COLLECT
    }

    /**
     * @param readOnly      let the callbacks read from replicas, with the given consistency
     * @param consistency   used when {@code readOnly} is set
     * @param timeout       longest time to wait for all shards
     */
    public record FanOutOptions(boolean readOnly, KurinReadOnly.Consistency consistency, Duration timeout, FailurePolicy failurePolicy) {

        public static FanOutOptions defaults() {
            return new FanOutOptions(false, KurinReadOnly.Consistency.READ_YOUR_WRITES, Duration.ofSeconds(30), FailurePolicy.FAIL_FAST);
        }

        public FanOutOptions readOnly(KurinReadOnly.Consistency consistency) {
            return new FanOutOptions(true, consistency, timeout, failurePolicy);
        }

        public FanOutOptions timeout(Duration timeout) {
            return new FanOutOptions(readOnly, consistency, timeout, failurePolicy);
        }

        public FanOutOptions failurePolicy(FailurePolicy failurePolicy) {
            return new FanOutOptions(readOnly, consistency, timeout, failurePolicy);
        }
    }

    /**
     * @param error null if the shard succeeded
     */
    public record ShardResult<T>(String shard, T value, Throwable error, Duration duration) {
        public boolean succeeded() {
            return error == null;
        }
    }

    public static final class ShardResults<T> {
        private final Map<String, ShardResult<T>> results;
        private final Duration duration;

        ShardResults(Map<String, ShardResult<T>> results, Duration duration) {
            this.results = Map.copyOf(results);
            this.duration = duration;
        }

        public ShardResult<T> get(String shard) {
            return results.get(shard);
        }

        public Map<String, ShardResult<T>> byShard() {
            return results;
        }

        /**
         * @return the values of the shards that succeeded
         */
        public List<T> values() {
            return results.values().stream().filter(ShardResult::succeeded).map(ShardResult::value).toList();
        }

        public Map<String, Throwable> failures() {
            Map<String, Throwable> failures = new LinkedHashMap<>();
            results.values().stream().filter(result -> !result.succeeded()).forEach(result -> failures.put(result.shard(), result.error()));
            return failures;
        }

        public boolean allSucceeded() {
            return results.values().stream().allMatch(ShardResult::succeeded);
        }

        public Duration duration() {
            return duration;
        }
    }

    /**
     * Thrown by a FAIL_FAST fan-out: one shard failed and the rest were cancelled.
     */
    public static class KurinShardExecutionException extends KurinRoutingException {
        private final String shard;

        public KurinShardExecutionException(String shard, Throwable cause) {
            super("Operation failed on shard '" + shard + "': " + cause, cause);
            this.shard = shard;
        }

        public String shard() {
            return shard;
        }
    }

    /**
     * Observes fan-outs, for metrics.
     */
    public interface FanOutListener {
        void onFanOut(int shards, int failed, Duration duration);
    }

    private final KurinShardRouter router;
    private final ExecutorService executor;
    private final List<FanOutListener> listeners = new ArrayList<>();

    /**
     * @param parallelism largest number of shards worked on at the same time, across all concurrent fan-outs
     */
    public KurinShardOperations(KurinShardRouter router, int parallelism) {
        this.router = router;
        AtomicInteger threadNumber = new AtomicInteger();
        this.executor = Executors.newFixedThreadPool(Math.max(1, parallelism), task -> {
            Thread thread = new Thread(task, "kurin-fanout-" + threadNumber.incrementAndGet());
            thread.setDaemon(true);
            return thread;
        });
    }

    public void addListener(FanOutListener listener) {
        listeners.add(listener);
    }

    /**
     * Runs the callback on the calling thread, routed to the given shard.
     */
    public <T> T executeOnShard(String shard, ShardCallback<T> callback) throws Exception {
        if (!router.shards().contains(shard)) {
            throw new KurinRoutingException("Unknown shard '" + shard + "'. Configured shards: " + router.shards());
        }
        try (KurinRoutingContext.Scope ignored = KurinRoutingContext.shard(shard)) {
            return callback.execute(shard);
        }
    }

    /**
     * Runs the callback on the calling thread, routed to the shard that owns the key.
     *
     * @param key the shard key, or an object with a {@code @KurinShardKey} field
     */
    public <T> T executeForKey(Object key, ShardCallback<T> callback) throws Exception {
        return executeOnShard(router.route(key), callback);
    }

    public <T> ShardResults<T> executeOnAllShards(ShardCallback<T> callback) {
        return executeOnAllShards(callback, FanOutOptions.defaults());
    }

    /**
     * Runs the callback once per shard, in parallel.
     *
     * @throws KurinShardExecutionException with FAIL_FAST, when a shard fails or the timeout passes
     */
    public <T> ShardResults<T> executeOnAllShards(ShardCallback<T> callback, FanOutOptions options) {
        long startedNanos = System.nanoTime();
        ConsistencyToken callerToken = KurinConsistency.current();
        boolean failFast = options.failurePolicy() == FailurePolicy.FAIL_FAST;

        Map<String, CompletableFuture<ShardResult<T>>> outcomes = new LinkedHashMap<>();
        List<Future<?>> tasks = new ArrayList<>();
        // Completed by the first failing shard, so that FAIL_FAST does not wait for the slow ones
        CompletableFuture<ShardResult<T>> firstFailure = new CompletableFuture<>();

        for (String shard : router.shards()) {
            CompletableFuture<ShardResult<T>> outcome = new CompletableFuture<>();
            outcomes.put(shard, outcome);
            tasks.add(executor.submit(() -> {
                ShardResult<T> result = runOnShard(shard, callback, options, callerToken);
                if (!result.succeeded()) {
                    firstFailure.complete(result);
                }
                outcome.complete(result);
            }));
        }

        CompletableFuture<Void> all = CompletableFuture.allOf(outcomes.values().toArray(CompletableFuture[]::new));
        boolean timedOut = false;
        try {
            (failFast ? CompletableFuture.anyOf(all, firstFailure) : all).get(options.timeout().toNanos(), TimeUnit.NANOSECONDS);
        } catch (TimeoutException e) {
            timedOut = true;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            timedOut = true;
        } catch (ExecutionException e) {
            throw new IllegalStateException("Shard tasks report failures as results, never as exceptions", e);
        }

        // Taken before the stragglers are interrupted, so that they are reported as timed out, not as interrupted
        Map<String, ShardResult<T>> results = new LinkedHashMap<>();
        outcomes.forEach((shard, outcome) -> results.put(shard, outcome.getNow(new ShardResult<>(shard, null,
                new TimeoutException("Shard '" + shard + "' did not finish within " + options.timeout().toMillis() + " ms"),
                Duration.ofNanos(System.nanoTime() - startedNanos)))));

        if (timedOut || (failFast && firstFailure.isDone())) {
            // Interrupts the shards that are still working
            tasks.forEach(task -> task.cancel(true));
        }

        int failed = (int) results.values().stream().filter(result -> !result.succeeded()).count();
        notifyListeners(results.size(), failed, startedNanos);

        if (failFast && failed > 0) {
            ShardResult<T> failure = firstFailure.isDone() ? firstFailure.join()
                    : results.values().stream().filter(result -> !result.succeeded()).findFirst().orElseThrow();
            throw new KurinShardExecutionException(failure.shard(), failure.error());
        }
        return new ShardResults<>(results, Duration.ofNanos(System.nanoTime() - startedNanos));
    }

    // Runs on a pool thread: everything bound to the thread is set here and removed here
    private <T> ShardResult<T> runOnShard(String shard, ShardCallback<T> callback, FanOutOptions options, ConsistencyToken callerToken) {
        long startedNanos = System.nanoTime();
        ConsistencyToken previousToken = KurinConsistency.bind(callerToken);
        try (KurinRoutingContext.Scope shardScope = KurinRoutingContext.shard(shard);
             KurinRoutingContext.Scope readScope = options.readOnly() ? KurinRoutingContext.readOnly(options.consistency()) : () -> {
             }) {
            T value = callback.execute(shard);
            return new ShardResult<>(shard, value, null, Duration.ofNanos(System.nanoTime() - startedNanos));
        } catch (Throwable t) {
            return new ShardResult<>(shard, null, t, Duration.ofNanos(System.nanoTime() - startedNanos));
        } finally {
            KurinConsistency.restore(previousToken);
        }
    }

    private void notifyListeners(int shards, int failed, long startedNanos) {
        Duration duration = Duration.ofNanos(System.nanoTime() - startedNanos);
        listeners.forEach(listener -> listener.onFanOut(shards, failed, duration));
    }

    @Override
    public void close() {
        executor.shutdownNow();
    }
}
