package org.kurin.kurincachespringbootstarter;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import org.kurin.kurinspringbootstarter.observability.KurinEndpointContributor;
import org.kurin.kurintieredcache.api.CacheResult;
import org.kurin.kurintieredcache.api.KurinCacheObserver;
import org.kurin.kurintieredcache.api.StampedeMode;
import org.kurin.kurintieredcache.core.KurinCacheManager;
import org.kurin.kurintieredcache.core.TieredCache;
import org.kurin.kurintieredcache.tier.CircuitBreaker;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Optional metrics, health and Actuator section of the cache.
 * <p>
 * Meters: {@code kurin.cache.gets{cache,tier,result}}, {@code kurin.cache.loads{cache,result}},
 * {@code kurin.cache.load.duration{cache}}, {@code kurin.cache.evictions{cache,reason}},
 * {@code kurin.cache.stampede.waits{cache,mode}}, {@code kurin.cache.l3.circuit.state{cache}}
 * (0 closed, 1 half-open, 2 open).
 */
@AutoConfiguration(before = KurinCacheAutoConfiguration.class, afterName = {
        "org.springframework.boot.actuate.autoconfigure.metrics.MetricsAutoConfiguration",
        "org.springframework.boot.actuate.autoconfigure.metrics.CompositeMeterRegistryAutoConfiguration",
        "org.springframework.boot.actuate.autoconfigure.metrics.export.simple.SimpleMetricsExportAutoConfiguration"})
@ConditionalOnClass(KurinCacheManager.class)
public class KurinCacheObservabilityAutoConfiguration {

    /**
     * An observer bean: the cache manager picks it up like any observer of the application.
     */
    @Configuration(proxyBeanMethods = false)
    @ConditionalOnClass(MeterRegistry.class)
    @ConditionalOnBean(MeterRegistry.class)
    static class MetricsConfiguration {
        @Bean
        KurinCacheObserver kurinCacheMetrics(MeterRegistry registry) {
            Map<String, AtomicInteger> circuits = new ConcurrentHashMap<>();
            return new KurinCacheObserver() {
                @Override
                public void onLookup(String cache, String key, CacheResult result) {
                    String tier = switch (result) {
                        case HIT_L1 -> "L1";
                        case HIT_L2 -> "L2";
                        case HIT_L3 -> "L3";
                        case STALE, MISS -> "none";
                    };
                    String outcome = switch (result) {
                        case HIT_L1, HIT_L2, HIT_L3 -> "hit";
                        case STALE -> "stale";
                        case MISS -> "miss";
                    };
                    registry.counter("kurin.cache.gets", "cache", cache, "tier", tier, "result", outcome).increment();
                }

                @Override
                public void onLoad(String cache, String key, Duration duration, boolean success) {
                    registry.counter("kurin.cache.loads", "cache", cache, "result", success ? "success" : "failure").increment();
                    registry.timer("kurin.cache.load.duration", "cache", cache).record(duration.toNanos(), TimeUnit.NANOSECONDS);
                }

                @Override
                public void onEvict(String cache, String key, String reason) {
                    registry.counter("kurin.cache.evictions", "cache", cache, "reason", reason).increment();
                }

                @Override
                public void onStampedeWait(String cache, String key, StampedeMode mode, Duration waited) {
                    registry.counter("kurin.cache.stampede.waits", "cache", cache, "mode", mode.name()).increment();
                }

                @Override
                public void onExternalStoreCircuit(String cache, String state) {
                    circuits.computeIfAbsent(cache, name -> {
                        AtomicInteger value = new AtomicInteger();
                        Gauge.builder("kurin.cache.l3.circuit.state", value, AtomicInteger::get).tag("cache", name)
                                .description("0 closed, 1 half-open, 2 open").register(registry);
                        return value;
                    }).set(switch (CircuitBreaker.State.valueOf(state)) {
                        case CLOSED -> 0;
                        case HALF_OPEN -> 1;
                        case OPEN -> 2;
                    });
                }
            };
        }
    }

    private static Map<String, Object> describeCaches(KurinCacheManager manager) {
        Map<String, Object> caches = new LinkedHashMap<>();
        for (TieredCache cache : manager.caches()) {
            Map<String, Object> details = new LinkedHashMap<>();
            details.put("tiers", cache.tiers().toString());
            details.put("ttl", cache.settings().ttl().toString());
            details.put("memoryEntries", cache.memoryEntries());
            if (cache.externalCircuitState() != null) {
                details.put("externalStoreCircuit", cache.externalCircuitState().name());
                details.put("externalStoreShared", cache.hasSharedTier());
            }
            caches.put(cache.name(), details);
        }
        return caches;
    }

    @Configuration(proxyBeanMethods = false)
    @ConditionalOnClass(HealthIndicator.class)
    static class HealthConfiguration {
        /**
         * Never DOWN: the external tier is optional by design, and with its circuit open the cache keeps working
         * from memory, disk and the source. An open circuit is reported as a degraded detail.
         */
        @Bean
        @ConditionalOnMissingBean(name = "kurinCacheHealthIndicator")
        HealthIndicator kurinCacheHealthIndicator(KurinCacheManager manager) {
            return () -> {
                boolean degraded = manager.caches().stream()
                        .anyMatch(cache -> cache.externalCircuitState() != null && cache.externalCircuitState() != CircuitBreaker.State.CLOSED);
                return Health.up().withDetail("degraded", degraded).withDetail("caches", describeCaches(manager)).build();
            };
        }
    }

    @Configuration(proxyBeanMethods = false)
    @ConditionalOnClass(KurinEndpointContributor.class)
    static class EndpointConfiguration {
        @Bean
        KurinEndpointContributor kurinCacheEndpointContributor(KurinCacheManager manager) {
            return new KurinEndpointContributor() {
                @Override
                public String name() {
                    return "caches";
                }

                @Override
                public Object describe() {
                    return describeCaches(manager);
                }
            };
        }
    }
}
