package org.kurin.routing;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.kurin.routing.consistency.ConsistencyToken;
import org.kurin.routing.consistency.KurinConsistency;
import org.kurin.routing.shard.ConsistentHashRing;
import org.kurin.routing.shard.ConsistentHashShardResolver;
import org.kurin.routing.shard.KurinShardOperations;
import org.kurin.routing.shard.KurinShardOperations.FailurePolicy;
import org.kurin.routing.shard.KurinShardOperations.FanOutOptions;
import org.kurin.routing.shard.KurinShardOperations.KurinShardExecutionException;
import org.kurin.routing.shard.KurinShardOperations.ShardResults;
import org.kurin.routing.shard.KurinShardRouter;
import org.kurin.routing.shard.RingDiff;

import java.time.Duration;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.within;

class ShardingTest {

    public record Order(@KurinShardKey String customerId, double amount) {
    }

    private KurinShardOperations operations;

    @AfterEach
    void stop() {
        if (operations != null) {
            operations.close();
        }
    }

    private static Map<String, Integer> weights(Object... nameAndWeight) {
        Map<String, Integer> weights = new LinkedHashMap<>();
        for (int i = 0; i < nameAndWeight.length; i += 2) {
            weights.put((String) nameAndWeight[i], (Integer) nameAndWeight[i + 1]);
        }
        return weights;
    }

    // Spec test 30
    @Test
    void keysAreDistributedInProportionToShardWeights() {
        ConsistentHashRing ring = new ConsistentHashRing(weights("small", 1, "medium", 2, "large", 3), 160);
        int total = 100_000;
        Map<String, Integer> counts = new HashMap<>();
        for (int i = 0; i < total; i++) {
            counts.merge(ring.nodeFor("customer-" + i), 1, Integer::sum);
        }

        assertThat(counts.get("small") / (double) total).isCloseTo(1 / 6.0, within(0.05));
        assertThat(counts.get("medium") / (double) total).isCloseTo(2 / 6.0, within(0.05));
        assertThat(counts.get("large") / (double) total).isCloseTo(3 / 6.0, within(0.05));
    }

    // Spec test 30
    @Test
    void addingAShardMovesOnlyTheKeysThatGoToIt() {
        ConsistentHashRing before = new ConsistentHashRing(List.of("A", "B"), 160);
        ConsistentHashRing after = new ConsistentHashRing(List.of("A", "B", "C"), 160);
        RingDiff diff = before.diff(after);

        assertThat(diff.ranges()).isNotEmpty().allSatisfy(range -> assertThat(range.newOwner()).isEqualTo("C"));

        int total = 100_000;
        int moved = 0;
        for (int i = 0; i < total; i++) {
            String key = "customer-" + i;
            long hash = ConsistentHashRing.hash(key);
            boolean inRange = diff.ranges().stream().anyMatch(range -> range.contains(hash));
            assertThat(diff.moves(key)).isEqualTo(inRange);
            if (diff.moves(key)) {
                moved++;
                assertThat(diff.newOwner(key)).isEqualTo("C");
                assertThat(diff.oldOwner(key)).isEqualTo(before.nodeFor(key));
            } else {
                assertThat(after.nodeFor(key)).isEqualTo(before.nodeFor(key));
            }
        }
        // About a third of the keys belongs to the new shard, and the diff predicts that share
        assertThat(moved / (double) total).isCloseTo(1 / 3.0, within(0.05));
        assertThat(diff.movedFraction()).isCloseTo(moved / (double) total, within(0.02));
        assertThat(before.diff(before).ranges()).isEmpty();
    }

    @Test
    void routerUsesTheAnnotatedFieldOrTheKeyItself() {
        KurinShardRouter router = new KurinShardRouter(Set.of("A", "B"), new ConsistentHashShardResolver(Map.of(), 160));

        assertThat(router.route(new Order("customer-7", 10))).isEqualTo(router.route("customer-7"));
        assertThatThrownBy(() -> router.route(new Order(null, 1))).isInstanceOf(KurinRoutingException.class);

        KurinShardRouter broken = new KurinShardRouter(Set.of("A", "B"), (key, shards) -> "Z");
        assertThatThrownBy(() -> broken.route("x")).isInstanceOf(KurinRoutingException.class).hasMessageContaining("'Z'");
    }

    private KurinShardOperations operations(String... shards) {
        operations = new KurinShardOperations(new KurinShardRouter(Set.of(shards), new ConsistentHashShardResolver(Map.of(), 160)), 4);
        return operations;
    }

    // Spec test 28
    @Test
    void collectReportsEveryShardAndLeavesNoContextBehind() {
        KurinShardOperations ops = operations("A", "B", "C");
        Map<String, String> contexts = new ConcurrentHashMap<>();

        ShardResults<String> results = ops.executeOnAllShards(shard -> {
            contexts.put(shard, KurinRoutingContext.shard() + "/" + KurinRoutingContext.requestedType());
            if (shard.equals("B")) {
                throw new IllegalStateException("B is broken");
            }
            return "ok-" + shard;
        }, FanOutOptions.defaults().failurePolicy(FailurePolicy.COLLECT).readOnly(KurinReadOnly.Consistency.EVENTUAL));

        assertThat(contexts).containsOnly(Map.entry("A", "A/REPLICA"), Map.entry("B", "B/REPLICA"), Map.entry("C", "C/REPLICA"));
        assertThat(results.allSucceeded()).isFalse();
        assertThat(results.values()).containsExactlyInAnyOrder("ok-A", "ok-C");
        assertThat(results.failures()).containsOnlyKeys("B");
        assertThat(results.failures().get("B")).hasMessage("B is broken");
        assertThat(results.get("A").duration()).isNotNull();

        // The pool threads are clean again: the next fan-out on them sees no leftover context
        assertThat(KurinRoutingContext.shard()).isNull();
        Map<String, Object> leftovers = new ConcurrentHashMap<>();
        for (int round = 0; round < 3; round++) {
            ops.executeOnAllShards(shard -> {
                if (KurinRoutingContext.requestedType() != null) {
                    leftovers.put(shard, KurinRoutingContext.requestedType());
                }
                return null;
            });
        }
        assertThat(leftovers).isEmpty();
    }

    // Spec test 28
    @Test
    void failFastThrowsTheFirstFailureWithoutWaitingForSlowShards() {
        KurinShardOperations ops = operations("A", "B", "C");
        CountDownLatch slowShardsStarted = new CountDownLatch(2);
        AtomicBoolean interrupted = new AtomicBoolean();

        long started = System.nanoTime();
        assertThatThrownBy(() -> ops.executeOnAllShards(shard -> {
            if (shard.equals("B")) {
                slowShardsStarted.await(5, TimeUnit.SECONDS);
                throw new IllegalStateException("B is broken");
            }
            slowShardsStarted.countDown();
            try {
                new CountDownLatch(1).await(30, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                interrupted.set(true);
                throw e;
            }
            return "late";
        }))
                .isInstanceOfSatisfying(KurinShardExecutionException.class, failure -> assertThat(failure.shard()).isEqualTo("B"))
                .hasRootCauseMessage("B is broken");

        assertThat(Duration.ofNanos(System.nanoTime() - started)).isLessThan(Duration.ofSeconds(10));
        org.awaitility.Awaitility.await().atMost(Duration.ofSeconds(5)).untilTrue(interrupted);
    }

    @Test
    void fanOutTimeoutIsAFailureOfTheShardsThatDidNotFinish() {
        KurinShardOperations ops = operations("A", "B");

        ShardResults<String> results = ops.executeOnAllShards(shard -> {
            if (shard.equals("B")) {
                new CountDownLatch(1).await(30, TimeUnit.SECONDS);
            }
            return shard;
        }, FanOutOptions.defaults().failurePolicy(FailurePolicy.COLLECT).timeout(Duration.ofMillis(200)));

        assertThat(results.values()).containsExactly("A");
        assertThat(results.failures().get("B")).isInstanceOf(java.util.concurrent.TimeoutException.class);
    }

    @Test
    void singleShardExecutionRunsOnTheCallerWithTheShardSelected() throws Exception {
        KurinShardOperations ops = operations("A", "B");
        ConsistencyToken token = ConsistencyToken.empty().with("A", "42");

        String selected = ops.executeOnShard("A", shard -> KurinRoutingContext.shard());
        assertThat(selected).isEqualTo("A");
        String owner = ops.executeForKey(new Order("customer-1", 5), shard -> KurinRoutingContext.shard());
        assertThat(owner).isIn("A", "B");
        assertThat(KurinRoutingContext.shard()).isNull();
        assertThatThrownBy(() -> ops.executeOnShard("Z", shard -> shard)).isInstanceOf(KurinRoutingException.class);

        // The consistency token of the caller is visible in every shard task
        KurinConsistency.withToken(token, () ->
                assertThat(ops.executeOnAllShards(shard -> KurinConsistency.current().position("A")).values()).containsOnly("42"));
    }
}
