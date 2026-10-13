package org.kurin.kurinlocksspringbootstarter;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import org.kurin.api.ReadConsistency;
import org.kurin.kurinlocks.api.KurinLockManager;
import org.kurin.kurinlocks.command.LockCommands.CountLocks;
import org.kurin.kurinlocks.job.KurinJobs;
import org.kurin.kurinspringbootstarter.observability.KurinEndpointContributor;
import org.kurin.raft.KurinNode;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Optional metrics and Actuator section of locks and jobs.
 * <p>
 * Meters: {@code kurin.locks.acquire{result}}, {@code kurin.locks.wait.duration}, {@code kurin.locks.held},
 * {@code kurin.jobs.claims{job,result}}, {@code kurin.jobs.duration{job,status}}.
 */
@AutoConfiguration(after = KurinLocksAutoConfiguration.class, afterName = {
        "org.springframework.boot.actuate.autoconfigure.metrics.MetricsAutoConfiguration",
        "org.springframework.boot.actuate.autoconfigure.metrics.CompositeMeterRegistryAutoConfiguration",
        "org.springframework.boot.actuate.autoconfigure.metrics.export.simple.SimpleMetricsExportAutoConfiguration"})
@ConditionalOnBean(KurinLockManager.class)
public class KurinLocksObservabilityAutoConfiguration {

    @Configuration(proxyBeanMethods = false)
    @ConditionalOnClass(MeterRegistry.class)
    @ConditionalOnBean(MeterRegistry.class)
    static class MetricsConfiguration {
        @Bean
        InitializingBean kurinCoordinationMetrics(MeterRegistry registry, KurinLockManager lockManager, KurinJobs jobs) {
            return () -> {
                AtomicInteger held = new AtomicInteger();
                Gauge.builder("kurin.locks.held", held, AtomicInteger::get)
                        .description("Locks currently held through this instance").register(registry);
                lockManager.addListener(new KurinLockManager.LockListener() {
                    @Override
                    public void onAcquire(String key, boolean acquired, Duration waited) {
                        registry.counter("kurin.locks.acquire", "result", acquired ? "acquired" : "busy").increment();
                        registry.timer("kurin.locks.wait.duration").record(waited.toNanos(), TimeUnit.NANOSECONDS);
                        if (acquired) {
                            held.incrementAndGet();
                        }
                    }

                    @Override
                    public void onRelease(String key) {
                        held.decrementAndGet();
                    }
                });
                jobs.addListener(new KurinJobs.JobListener() {
                    @Override
                    public void onClaim(String jobName, boolean granted) {
                        registry.counter("kurin.jobs.claims", "job", jobName, "result", granted ? "granted" : "denied").increment();
                    }

                    @Override
                    public void onRun(String jobName, boolean success, Duration duration) {
                        registry.timer("kurin.jobs.duration", "job", jobName, "status", success ? "succeeded" : "failed")
                                .record(duration.toNanos(), TimeUnit.NANOSECONDS);
                    }
                });
            };
        }
    }

    @Configuration(proxyBeanMethods = false)
    @ConditionalOnClass(KurinEndpointContributor.class)
    static class EndpointConfiguration {
        @Bean
        KurinEndpointContributor kurinLocksEndpointContributor(KurinNode node, KurinJobs jobs) {
            return new KurinEndpointContributor() {
                @Override
                public String name() {
                    return "coordination";
                }

                @Override
                public Object describe() {
                    Map<String, Object> details = new LinkedHashMap<>();
                    // As this instance sees it right now; no round trip to the leader
                    details.put("locks", readLocally(node, new CountLocks()));
                    Map<String, Object> history = new LinkedHashMap<>();
                    jobs.knownJobs().forEach(job -> history.put(job,
                            readLocally(node, new org.kurin.kurinlocks.job.JobCommands.JobHistory(job))));
                    details.put("jobs", history);
                    return details;
                }
            };
        }

        private static Object readLocally(KurinNode node, Object query) {
            try {
                return node.submitQuery(query, ReadConsistency.LOCAL).get(1, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return "interrupted";
            } catch (Exception e) {
                return "unavailable: " + e;
            }
        }
    }
}
