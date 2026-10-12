package org.kurin.kurintieredcache;

import org.junit.jupiter.api.Test;
import org.kurin.kurintieredcache.api.KurinCacheException;
import org.kurin.kurintieredcache.api.KurinCacheKeyException;
import org.kurin.kurintieredcache.codec.KryoCacheCodec;
import org.kurin.kurintieredcache.core.CacheEntry;
import org.kurin.kurintieredcache.core.KurinNullValue;
import org.kurin.kurintieredcache.key.CacheKeyRenderer;
import org.kurin.kurintieredcache.key.DefaultKeyGenerator;
import org.kurin.kurintieredcache.tier.CircuitBreaker;

import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class CacheKeyAndCodecTest {

    public enum Channel {WEB, STORE}

    public record Money(BigDecimal amount, String currency) {
    }

    public record Filter(String category, List<Channel> channels, Optional<Money> maxPrice) {
    }

    public record Product(UUID id, String name, Money price, List<String> tags, Map<String, Integer> stock, Instant updatedAt) {
    }

    public static class Opaque {
    }

    public static class Catalog {
        public Object find(String a, String b) {
            return null;
        }

        public Object search(Filter filter, int page) {
            return null;
        }

        public Object byHandle(Opaque handle) {
            return null;
        }
    }

    private final DefaultKeyGenerator generator = new DefaultKeyGenerator();

    private String key(String methodName, Object... arguments) {
        for (Method method : Catalog.class.getMethods()) {
            if (method.getName().equals(methodName)) {
                return generator.generate(new Catalog(), method, arguments);
            }
        }
        throw new IllegalArgumentException(methodName);
    }

    // Spec test 20, default keys
    @Test
    void differentArgumentsNeverShareADefaultKey() {
        // "Aa" and "BB" have the same hashCode, which made them share a key before
        assertThat("Aa".hashCode()).isEqualTo("BB".hashCode());
        assertThat(key("find", "Aa", "x")).isNotEqualTo(key("find", "BB", "x"));

        // Argument boundaries cannot be forged by the content of a string
        assertThat(key("find", "a,b", "c")).isNotEqualTo(key("find", "a", "b,c"));
        assertThat(key("find", "a\",\"b", "c")).isNotEqualTo(key("find", "a", "b\",\"c"));
        assertThat(key("find", null, "null")).isNotEqualTo(key("find", "null", null));
        assertThat(key("find", "", null)).isNotEqualTo(key("find", null, ""));

        assertThat(key("find", "Aa", "x")).isEqualTo("Catalog.find(\"Aa\",\"x\")");
    }

    @Test
    void recordsCollectionsAndValueTypesRenderDeterministically() {
        Filter filter = new Filter("books", List.of(Channel.WEB, Channel.STORE), Optional.of(new Money(new BigDecimal("9.99"), "EUR")));

        assertThat(key("search", filter, 2))
                .isEqualTo("Catalog.search(Filter(\"books\",[Channel.WEB,Channel.STORE],Optional[Money(9.99,\"EUR\")]),2)");
        assertThat(key("search", filter, 2)).isNotEqualTo(key("search", filter, 3));

        // Sets and maps render the same whatever their iteration order
        Set<String> oneOrder = new LinkedHashSet<>(List.of("b", "a"));
        Set<String> otherOrder = new LinkedHashSet<>(List.of("a", "b"));
        assertThat(CacheKeyRenderer.render(oneOrder)).isEqualTo(CacheKeyRenderer.render(otherOrder));
        Map<String, Integer> oneMap = new LinkedHashMap<>();
        oneMap.put("x", 1);
        oneMap.put("y", 2);
        Map<String, Integer> otherMap = new LinkedHashMap<>();
        otherMap.put("y", 2);
        otherMap.put("x", 1);
        assertThat(CacheKeyRenderer.render(oneMap)).isEqualTo(CacheKeyRenderer.render(otherMap));

        assertThat(CacheKeyRenderer.render(LocalDate.of(2026, 1, 2))).isEqualTo("2026-01-02");
        assertThat(CacheKeyRenderer.render(new int[]{1, 2})).isEqualTo("[1,2]");
        assertThat(CacheKeyRenderer.render(List.of("1"))).isNotEqualTo(CacheKeyRenderer.render(List.of(1)));
    }

    @Test
    void typesWithoutAKnownTextFormAreRefused() {
        assertThatThrownBy(() -> key("byHandle", new Opaque()))
                .isInstanceOf(KurinCacheKeyException.class)
                .hasMessageContaining("Catalog.byHandle")
                .hasMessageContaining(Opaque.class.getName())
                .hasMessageContaining("explicit key expression");
    }

    @Test
    void codecRoundTripsValuesWithTheirLifetime() {
        KryoCacheCodec codec = new KryoCacheCodec(List.of());
        Product product = new Product(UUID.randomUUID(), "Lamp", new Money(new BigDecimal("19.90"), "EUR"),
                List.of("home", "light"), Map.of("berlin", 3), Instant.parse("2026-03-04T05:06:07Z"));

        CacheEntry full = new CacheEntry(product, 1_000, 61_000, 91_000);
        CacheEntry decoded = codec.decode(codec.encode(full));
        assertThat(decoded).usingRecursiveComparison().isEqualTo(full);
        assertThat(codec.deadAtMs(codec.encode(full))).isEqualTo(91_000);

        CacheEntry eternal = new CacheEntry("text", 5, 0, 0);
        assertThat(codec.decode(codec.encode(eternal))).isEqualTo(eternal);
        assertThat(codec.deadAtMs(codec.encode(eternal))).isZero();

        CacheEntry nullEntry = new CacheEntry(KurinNullValue.INSTANCE, 7, 107, 0);
        CacheEntry decodedNull = codec.decode(codec.encode(nullEntry));
        assertThat(decodedNull.isNullMarker()).isTrue();
        assertThat(decodedNull.value()).isNull();
        assertThat(decodedNull.expiresAtMs()).isEqualTo(107);

        List<Object> mixed = new ArrayList<>(List.of(1, "two", Duration.ofSeconds(3), Collections.unmodifiableList(new ArrayList<>(List.of("x")))));
        assertThat(codec.decode(codec.encode(new CacheEntry(mixed, 1, 0, 0))).payload()).isEqualTo(mixed);
    }

    @Test
    void codecRefusesClassesOutsideTheAllowedPackages() {
        KryoCacheCodec open = new KryoCacheCodec(List.of());
        KryoCacheCodec restricted = new KryoCacheCodec(List.of("org.kurin.kurintieredcache"));
        KryoCacheCodec elsewhere = new KryoCacheCodec(List.of("com.example.dto"));
        Money money = new Money(BigDecimal.ONE, "EUR");

        byte[] bytes = open.encode(new CacheEntry(money, 1, 0, 0));
        assertThat(restricted.decode(bytes).payload()).isEqualTo(money);

        // Bytes naming a class outside the list are rejected on read, as are such values on write
        assertThatThrownBy(() -> elsewhere.decode(bytes))
                .isInstanceOf(KurinCacheException.class)
                .hasMessageContaining(Money.class.getName())
                .hasMessageContaining("allowed-packages");
        assertThatThrownBy(() -> elsewhere.encode(new CacheEntry(money, 1, 0, 0))).isInstanceOf(KurinCacheException.class);

        // JDK value and collection types need no listing
        CacheEntry jdkOnly = new CacheEntry(List.of(Map.of("k", new BigDecimal("1.5")), UUID.randomUUID()), 1, 0, 0);
        assertThat(elsewhere.decode(elsewhere.encode(jdkOnly)).payload()).isEqualTo(jdkOnly.payload());

        assertThatThrownBy(() -> open.decode(new byte[]{1, 2, 3})).isInstanceOf(KurinCacheException.class);
    }

    @Test
    void circuitBreakerLetsExactlyOneProbeThroughWhenHalfOpen() {
        List<CircuitBreaker.State> transitions = new ArrayList<>();
        CircuitBreaker breaker = new CircuitBreaker(3, Duration.ofMillis(50).toNanos(), transitions::add);

        for (int i = 0; i < 3; i++) {
            assertThat(breaker.tryAcquire()).isTrue();
            breaker.onFailure();
        }
        assertThat(breaker.state()).isEqualTo(CircuitBreaker.State.OPEN);
        assertThat(breaker.tryAcquire()).isFalse();

        org.awaitility.Awaitility.await().atMost(Duration.ofSeconds(5)).until(() -> breaker.state() == CircuitBreaker.State.HALF_OPEN);
        assertThat(breaker.tryAcquire()).isTrue();
        assertThat(breaker.tryAcquire()).isFalse();
        assertThat(breaker.tryAcquire()).isFalse();

        // A failed probe opens the circuit again for a full period
        breaker.onFailure();
        assertThat(breaker.tryAcquire()).isFalse();
        org.awaitility.Awaitility.await().atMost(Duration.ofSeconds(5)).until(() -> breaker.state() == CircuitBreaker.State.HALF_OPEN);
        assertThat(breaker.tryAcquire()).isTrue();
        breaker.onSuccess();

        assertThat(breaker.state()).isEqualTo(CircuitBreaker.State.CLOSED);
        assertThat(breaker.tryAcquire()).isTrue();
        assertThat(transitions).containsExactly(CircuitBreaker.State.OPEN, CircuitBreaker.State.HALF_OPEN, CircuitBreaker.State.OPEN,
                CircuitBreaker.State.HALF_OPEN, CircuitBreaker.State.CLOSED);
    }
}
