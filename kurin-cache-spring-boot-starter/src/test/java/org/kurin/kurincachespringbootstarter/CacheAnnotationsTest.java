package org.kurin.kurincachespringbootstarter;

import jakarta.servlet.FilterChain;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.kurin.kurinlocksspringbootstarter.KurinLocksAutoConfiguration;
import org.kurin.kurinspringbootstarter.starter.KurinAutoConfiguration;
import org.kurin.kurintieredcache.api.CacheKeyProvider;
import org.kurin.kurintieredcache.api.KurinCacheEvict;
import org.kurin.kurintieredcache.api.KurinCacheKeyException;
import org.kurin.kurintieredcache.api.KurinCachePut;
import org.kurin.kurintieredcache.api.KurinCacheable;
import org.kurin.kurintieredcache.core.CacheEntry;
import org.kurin.kurintieredcache.core.KurinCacheManager;
import org.kurin.raft.KurinNode;
import org.kurin.test.TestPorts;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.autoconfigure.aop.AopAutoConfiguration;
import org.springframework.boot.test.context.assertj.AssertableApplicationContext;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.boot.test.context.runner.ContextConsumer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.mock.web.MockHttpServletResponse;

import java.nio.file.Path;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

class CacheAnnotationsTest {

    @TempDir
    Path directory;

    private ApplicationContextRunner runner() {
        return new ApplicationContextRunner()
                .withConfiguration(AutoConfigurations.of(KurinAutoConfiguration.class, KurinLocksAutoConfiguration.class,
                        KurinCacheAutoConfiguration.class, AopAutoConfiguration.class))
                .withPropertyValues(
                        "kurin.node.port=" + TestPorts.freePort(),
                        "kurin.node.storage=memory",
                        "kurin.node.heartbeat-interval=30ms",
                        "kurin.node.election-timeout-min=300ms",
                        "kurin.node.election-timeout-max=600ms",
                        "kurin.cache.data-dir=" + directory.toString().replace('\\', '/'),
                        "kurin.cache.defaults.ttl=5m",
                        "kurin.cache.caches.short.ttl=250ms",
                        "kurin.cache.caches.short.tiers=L1");
    }

    private static ContextConsumer<AssertableApplicationContext> withLeader(ContextConsumer<AssertableApplicationContext> test) {
        return context -> {
            assertThat(context).hasNotFailed();
            KurinNode node = context.getBean(KurinNode.class);
            await().atMost(Duration.ofSeconds(10)).until(() -> node.leadership().isPresent());
            test.accept(context);
        };
    }

    public record Lookup(String region, int page) {
    }

    public record Renamed(String id) implements CacheKeyProvider {
        @Override
        public String getCacheKey() {
            return id;
        }
    }

    public static class Opaque {
    }

    public static class Library {
        private final AtomicInteger loads = new AtomicInteger();

        public int loads() {
            return loads.get();
        }

        @KurinCacheable(cacheName = "titles")
        public String title(String isbn) {
            return "title-of-" + isbn + "#" + loads.incrementAndGet();
        }

        @KurinCacheable(cacheName = "pages")
        public String page(Lookup lookup, String sort) {
            return lookup + sort + "#" + loads.incrementAndGet();
        }

        @KurinCacheable(cacheName = "titles", key = "'author:' + #a0.toUpperCase() + ':' + #root.methodName")
        public String author(String isbn) {
            return "author#" + loads.incrementAndGet();
        }

        @KurinCacheable(cacheName = "titles", key = "#isbn.missingProperty")
        public String brokenKey(String isbn) {
            loads.incrementAndGet();
            return "never cached";
        }

        @KurinCacheable(cacheName = "titles", key = "#noSuchArgument")
        public String nullKey(String isbn) {
            loads.incrementAndGet();
            return "never cached";
        }

        @KurinCacheable(cacheName = "titles")
        public String byHandle(Opaque handle) {
            loads.incrementAndGet();
            return "never cached";
        }

        @KurinCacheable(cacheName = "conditional", key = "#id", condition = "#id.startsWith('c')", unless = "#result == 'volatile'")
        public String conditional(String id, String answer) {
            loads.incrementAndGet();
            return answer;
        }

        @KurinCacheable(cacheName = "short", key = "#id", cacheNulls = true, nullTtl = "100ms")
        public String shortLived(String id) {
            loads.incrementAndGet();
            return id.equals("none") ? null : "value";
        }

        @KurinCacheable(cacheName = "optional", key = "#id")
        public Optional<String> find(String id) {
            loads.incrementAndGet();
            return id.equals("none") ? Optional.empty() : Optional.of("found");
        }

        @KurinCachePut(cacheName = "titles", key = "#isbn")
        public String retitle(String isbn, String title) {
            return title;
        }

        @KurinCacheEvict(cacheNames = "titles", key = "#isbn", replicate = false)
        public void forget(String isbn) {
        }

        @KurinCacheEvict(cacheNames = {"titles", "pages"}, allEntries = true)
        public void forgetEverything() {
        }

        @KurinCacheEvict(cacheNames = "titles")
        public void renamed(Renamed event) {
        }
    }

    @Configuration(proxyBeanMethods = false)
    static class LibraryConfig {
        @Bean
        Library library() {
            return new Library();
        }
    }

    @Test
    void cacheableCachesByGeneratedAndExplicitKeys() {
        runner().withUserConfiguration(LibraryConfig.class).run(withLeader(context -> {
            Library library = context.getBean(Library.class);
            KurinCacheManager manager = context.getBean(KurinCacheManager.class);

            assertThat(library.title("111")).isEqualTo(library.title("111"));
            assertThat(library.loads()).isEqualTo(1);
            assertThat(manager.getCache("titles").get("Library.title(\"111\")").isHit()).isTrue();

            // Spec test 20: "Aa" and "BB" share a hashCode but not a cache entry
            assertThat(library.title("Aa")).isNotEqualTo(library.title("BB"));
            assertThat(library.page(new Lookup("eu", 1), "asc")).isNotEqualTo(library.page(new Lookup("eu", 2), "asc"));
            assertThat(library.page(new Lookup("eu", 1), "asc")).isEqualTo(library.page(new Lookup("eu", 1), "asc"));

            String author = library.author("x1");
            assertThat(library.author("X1")).isEqualTo(author);
            assertThat(manager.getCache("titles").get("author:X1:author").isHit()).isTrue();
        }));
    }

    // Spec test 20
    @Test
    void keyThatCannotBeDerivedIsAnErrorNeverASharedLiteral() {
        runner().withUserConfiguration(LibraryConfig.class).run(withLeader(context -> {
            Library library = context.getBean(Library.class);

            assertThatThrownBy(() -> library.brokenKey("1"))
                    .isInstanceOf(KurinCacheKeyException.class)
                    .hasMessageContaining("#isbn.missingProperty")
                    .hasMessageContaining("Library.brokenKey");
            assertThatThrownBy(() -> library.nullKey("1"))
                    .isInstanceOf(KurinCacheKeyException.class)
                    .hasMessageContaining("evaluated to null");
            assertThatThrownBy(() -> library.byHandle(new Opaque()))
                    .isInstanceOf(KurinCacheKeyException.class)
                    .hasMessageContaining("explicit key expression");
            assertThat(library.loads()).isZero();
        }));
    }

    @Test
    void conditionUnlessNullsAndOptionalsFollowTheirContracts() {
        runner().withUserConfiguration(LibraryConfig.class).run(withLeader(context -> {
            Library library = context.getBean(Library.class);
            KurinCacheManager manager = context.getBean(KurinCacheManager.class);

            // condition false: the cache is not consulted at all
            library.conditional("x1", "a");
            library.conditional("x1", "b");
            assertThat(library.loads()).isEqualTo(2);
            // unless true: returned but not stored
            assertThat(library.conditional("c1", "volatile")).isEqualTo("volatile");
            assertThat(library.conditional("c1", "stable")).isEqualTo("stable");
            assertThat(library.conditional("c1", "ignored")).isEqualTo("stable");
            assertThat(library.loads()).isEqualTo(4);

            // a cached null, with its own short lifetime
            assertThat(library.shortLived("none")).isNull();
            assertThat(library.shortLived("none")).isNull();
            assertThat(library.loads()).isEqualTo(5);
            CacheEntry negative = manager.getCache("short").get("none").entry();
            assertThat(negative.isNullMarker()).isTrue();
            assertThat(negative.expiresAtMs() - negative.createdAtMs()).isEqualTo(100);
            assertThat(library.shortLived("some")).isEqualTo("value");
            CacheEntry positive = manager.getCache("short").get("some").entry();
            assertThat(positive.expiresAtMs() - positive.createdAtMs()).isEqualTo(250);
            await().atMost(Duration.ofSeconds(5)).until(() -> !manager.getCache("short").get("some").isHit());

            // Optional results are cached as they are
            assertThat(library.find("none")).isEmpty();
            assertThat(library.find("none")).isEmpty();
            assertThat(library.find("one")).contains("found");
            assertThat(library.loads()).isEqualTo(8);
        }));
    }

    @Test
    void putAndEvictOnOrdinaryMethods() {
        runner().withUserConfiguration(LibraryConfig.class).run(withLeader(context -> {
            Library library = context.getBean(Library.class);
            KurinCacheManager manager = context.getBean(KurinCacheManager.class);

            library.retitle("7", "Dune");
            assertThat(manager.getCache("titles").get("7").entry().value()).isEqualTo("Dune");

            library.forget("7");
            assertThat(manager.getCache("titles").get("7").isHit()).isFalse();

            library.retitle("8", "Emma");
            library.retitle("9", "Ulysses");
            library.renamed(new Renamed("8"));
            await().atMost(Duration.ofSeconds(5)).until(() -> !manager.getCache("titles").get("8").isHit());
            assertThat(manager.getCache("titles").get("9").isHit()).isTrue();

            library.page(new Lookup("eu", 1), "asc");
            library.forgetEverything();
            await().atMost(Duration.ofSeconds(5)).until(() -> !manager.getCache("titles").get("9").isHit()
                    && !manager.getCache("pages").get("Library.page(Lookup(\"eu\",1),\"asc\")").isHit());
        }));
    }

    // ---------------------------------------------------------------- startup validation

    public static class NullsOnPrimitive {
        @KurinCacheable(cacheName = "c", cacheNulls = true)
        public int count(String id) {
            return 1;
        }
    }

    public static class EvictWithoutKey {
        @KurinCacheEvict(cacheNames = "c")
        public void update(String id) {
        }
    }

    public static class UnquotedKey {
        @KurinCacheable(cacheName = "c", key = "all items")
        public String all() {
            return "x";
        }
    }

    public static class BadTtl {
        @KurinCacheable(cacheName = "c", ttl = "forever")
        public String all() {
            return "x";
        }
    }

    public static class VoidCacheable {
        @KurinCacheable(cacheName = "c")
        public void nothing() {
        }
    }

    private void assertStartupFails(Class<?> beanType, String... expectedFragments) {
        runner().withBean(beanType).run(context -> {
            assertThat(context).hasFailed();
            Throwable failure = context.getStartupFailure();
            while (failure.getCause() != null && !(failure instanceof IllegalStateException && failure.getMessage().contains("@Kurin"))) {
                failure = failure.getCause();
            }
            assertThat(failure.getMessage()).contains(expectedFragments);
        });
    }

    @Test
    void misusedAnnotationsFailTheStartup() {
        assertStartupFails(NullsOnPrimitive.class, "cacheNulls", "primitive return type 'int'");
        assertStartupFails(EvictWithoutKey.class, "@KurinCacheEvict", "needs a key expression", "CacheKeyProvider");
        assertStartupFails(UnquotedKey.class, "invalid key expression", "must be quoted");
        assertStartupFails(BadTtl.class, "invalid ttl 'forever'");
        assertStartupFails(VoidCacheable.class, "void method has no result to cache");
    }

    // ---------------------------------------------------------------- trace header

    @Test
    void traceFilterReportsTheLookupsOfTheRequest() {
        runner().withUserConfiguration(LibraryConfig.class).run(withLeader(context -> {
            Library library = context.getBean(Library.class);
            KurinCacheTraceFilter filter = new KurinCacheTraceFilter();

            FilterChain handler = (request, response) -> {
                library.title("42");
                library.title("42");
                response.getWriter().write("body");
            };
            MockHttpServletResponse first = new MockHttpServletResponse();
            filter.doFilter(new MockHttpServletRequest("GET", "/titles/42"), first, handler);

            assertThat(first.getHeader(KurinCacheTraceFilter.HEADER)).isEqualTo("titles=MISS;titles=HIT_L1");
            assertThat(first.getContentAsString()).isEqualTo("body");

            // A request that touches no cache gets no header
            MockHttpServletResponse second = new MockHttpServletResponse();
            filter.doFilter(new MockHttpServletRequest("GET", "/ping"), second, (request, response) -> response.getWriter().write("pong"));
            assertThat(second.getHeader(KurinCacheTraceFilter.HEADER)).isNull();
            assertThat(second.getContentAsString()).isEqualTo("pong");
        }));
    }
}
