package org.kurin.kurinspringbootstarter;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.micrometer.observation.Observation;
import io.micrometer.observation.ObservationHandler;
import io.micrometer.observation.ObservationRegistry;
import org.junit.jupiter.api.Test;
import org.kurin.api.KurinCommand;
import org.kurin.api.KurinQuery;
import org.kurin.kurinspringbootstarter.observability.KurinEndpoint;
import org.kurin.kurinspringbootstarter.observability.KurinEndpointContributor;
import org.kurin.kurinspringbootstarter.observability.KurinObservabilityAutoConfiguration;
import org.kurin.kurinspringbootstarter.starter.KurinAutoConfiguration;
import org.kurin.kurinspringbootstarter.template.KurinTemplate;
import org.kurin.raft.KurinNode;
import org.kurin.test.TestPorts;
import org.slf4j.MDC;
import org.kurin.raft.KurinNodeHealth;
import org.kurin.raft.KurinNodeHealth.Status;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

class KurinObservabilityTest {

    public record Increment(int by) {
    }

    public record Current() {
    }

    public static class Counter {
        static final AtomicReference<Map<String, String>> MDC_DURING_APPLY = new AtomicReference<>();
        private int value;

        @KurinCommand
        public int increment(Increment command) {
            MDC_DURING_APPLY.set(MDC.getCopyOfContextMap());
            value += command.by();
            return value;
        }

        @KurinQuery
        public int current(Current query) {
            return value;
        }

        @org.kurin.api.KurinSnapshot
        public byte[] snapshot() {
            return java.nio.ByteBuffer.allocate(Integer.BYTES).putInt(value).array();
        }

        @org.kurin.api.KurinRestore
        public void restore(byte[] snapshot) {
            value = java.nio.ByteBuffer.wrap(snapshot).getInt();
        }
    }

    @Configuration(proxyBeanMethods = false)
    static class App {
        static final List<String> OBSERVED = new CopyOnWriteArrayList<>();

        @Bean
        Counter counter() {
            return new Counter();
        }

        @Bean
        MeterRegistry meterRegistry() {
            return new SimpleMeterRegistry();
        }

        @Bean
        ObservationRegistry observationRegistry() {
            ObservationRegistry registry = ObservationRegistry.create();
            registry.observationConfig().observationHandler(new ObservationHandler<Observation.Context>() {
                @Override
                public void onStop(Observation.Context context) {
                    OBSERVED.add(context.getName() + context.getLowCardinalityKeyValues());
                }

                @Override
                public boolean supportsContext(Observation.Context context) {
                    return true;
                }
            });
            return registry;
        }

        @Bean
        KurinEndpointContributor extraSection() {
            return new KurinEndpointContributor() {
                @Override
                public String name() {
                    return "extra";
                }

                @Override
                public Object describe() {
                    return Map.of("answer", 42);
                }
            };
        }
    }

    @Test
    void metricsObservationsHealthEndpointAndMdcAreAvailable() {
        App.OBSERVED.clear();
        new ApplicationContextRunner()
                .withConfiguration(AutoConfigurations.of(KurinAutoConfiguration.class, KurinObservabilityAutoConfiguration.class))
                .withUserConfiguration(App.class)
                .withPropertyValues(
                        "kurin.node.port=" + TestPorts.freePort(),
                        "kurin.node.storage=memory",
                        "kurin.node.heartbeat-interval=30ms",
                        "kurin.node.election-timeout-min=300ms",
                        "kurin.node.election-timeout-max=600ms")
                .run(context -> {
                    assertThat(context).hasNotFailed();
                    KurinNode node = context.getBean(KurinNode.class);
                    KurinTemplate template = context.getBean(KurinTemplate.class);
                    MeterRegistry meters = context.getBean(MeterRegistry.class);
                    await().atMost(Duration.ofSeconds(10)).until(() -> node.leadership().isPresent());

                    for (int i = 0; i < 5; i++) {
                        template.submitCommand(new Increment(1)).get(5, TimeUnit.SECONDS);
                    }
                    int current = template.<Integer>query(new Current()).get(5, TimeUnit.SECONDS);
                    assertThat(current).isEqualTo(5);

                    // Metrics
                    assertThat(meters.get("kurin.raft.role").gauge().value()).isEqualTo(2.0);
                    assertThat(meters.get("kurin.raft.term").gauge().value()).isEqualTo(node.status().term());
                    assertThat(meters.get("kurin.raft.applied.index").gauge().value()).isEqualTo(node.status().lastApplied());
                    assertThat(meters.get("kurin.raft.log.entries").gauge().value()).isGreaterThanOrEqualTo(6);
                    assertThat(meters.get("kurin.raft.elections").tag("result", "won").counter().count()).isEqualTo(1);
                    assertThat(meters.get("kurin.raft.leader.changes").counter().count()).isEqualTo(1);
                    assertThat(meters.get("kurin.raft.apply.duration").tag("type", "Increment").timer().count()).isEqualTo(5);
                    assertThat(meters.get("kurin.raft.commit.latency").timer().count()).isEqualTo(5);

                    // Observations, with their attributes
                    await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> assertThat(App.OBSERVED)
                            .filteredOn(name -> name.startsWith("kurin.command")).hasSize(5));
                    assertThat(App.OBSERVED).anyMatch(name -> name.startsWith("kurin.command") && name.contains("Increment"));
                    assertThat(App.OBSERVED).filteredOn(name -> name.startsWith("kurin.raft.apply")).hasSize(5);
                    assertThat(App.OBSERVED).anyMatch(name -> name.startsWith("kurin.query") && name.contains("Current"));

                    // Log context of the Raft thread
                    assertThat(Counter.MDC_DURING_APPLY.get())
                            .containsEntry("kurin.node", node.nodeId())
                            .containsEntry("kurin.role", "LEADER")
                            .containsEntry("kurin.term", Long.toString(node.status().term()));

                    // Actuator
                    KurinNodeHealth health = KurinNodeHealth.of(node, Duration.ofSeconds(10));
                    assertThat(health.status()).isEqualTo(Status.UP);
                    assertThat(health.details()).containsEntry("role", "LEADER").containsEntry("leaderId", node.nodeId());

                    Map<String, Object> endpoint = context.getBean(KurinEndpoint.class).kurin();
                    assertThat(endpoint).containsKeys("node", "extra");
                    assertThat(endpoint.get("extra")).isEqualTo(Map.of("answer", 42));
                    @SuppressWarnings("unchecked")
                    Map<String, Object> nodeSection = (Map<String, Object>) endpoint.get("node");
                    assertThat(nodeSection).containsEntry("leadershipEstablished", true).containsEntry("lastApplied", node.status().lastApplied());
                });
    }

    @Configuration(proxyBeanMethods = false)
    static class BreakableStorage {
        static final org.kurin.test.FaultInjectingStorage FAULTS = new org.kurin.test.FaultInjectingStorage();

        @Bean
        org.kurin.kurinspringbootstarter.starter.KurinNodeBuilderCustomizer breakableStorage() {
            return builder -> builder.storageDecorator(FAULTS::wrap);
        }
    }

    @Test
    void failedStorageIsReportedAsDownAndClearsAfterRepair() {
        BreakableStorage.FAULTS.setBroken(false);
        new ApplicationContextRunner()
                .withConfiguration(AutoConfigurations.of(KurinAutoConfiguration.class, KurinObservabilityAutoConfiguration.class))
                .withUserConfiguration(App.class, BreakableStorage.class)
                .withPropertyValues(
                        "kurin.node.port=" + TestPorts.freePort(),
                        "kurin.node.storage=memory",
                        "kurin.node.heartbeat-interval=30ms",
                        "kurin.node.election-timeout-min=300ms",
                        "kurin.node.election-timeout-max=600ms",
                        "kurin.node.storage-retry-min=20ms",
                        "kurin.node.storage-retry-max=100ms")
                .run(context -> {
                    KurinNode node = context.getBean(KurinNode.class);
                    KurinTemplate template = context.getBean(KurinTemplate.class);
                    java.util.function.Supplier<KurinNodeHealth> indicator = () -> KurinNodeHealth.of(node, Duration.ofSeconds(10));
                    MeterRegistry meters = context.getBean(MeterRegistry.class);
                    await().atMost(Duration.ofSeconds(10)).until(() -> node.leadership().isPresent());
                    assertThat(meters.get("kurin.raft.storage.failed").gauge().value()).isZero();

                    BreakableStorage.FAULTS.setBroken(true);
                    org.assertj.core.api.Assertions.assertThatThrownBy(() -> template.submitCommand(new Increment(1)).get(5, TimeUnit.SECONDS))
                            .hasCauseInstanceOf(org.kurin.api.exception.KurinStorageException.class);

                    await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
                        KurinNodeHealth health = indicator.get();
                        assertThat(health.status()).isEqualTo(Status.DOWN);
                        assertThat(String.valueOf(health.details().get("storageFailure"))).contains("Injected storage failure");
                    });
                    assertThat(meters.get("kurin.raft.storage.failed").gauge().value()).isEqualTo(1.0);

                    BreakableStorage.FAULTS.setBroken(false);
                    await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
                        assertThat(indicator.get().status()).isEqualTo(Status.UP);
                        assertThat(indicator.get().details()).doesNotContainKey("storageFailure");
                    });
                    assertThat(meters.get("kurin.raft.storage.failed").gauge().value()).isZero();
                    int value = template.<Integer>submitCommand(new Increment(1)).get(5, TimeUnit.SECONDS);
                    assertThat(value).isEqualTo(1);
                });
    }

    @Test
    void nodeWithoutALeaderGoesOutOfServiceAfterTheThreshold() {
        new ApplicationContextRunner()
                .withConfiguration(AutoConfigurations.of(KurinAutoConfiguration.class, KurinObservabilityAutoConfiguration.class))
                .withUserConfiguration(App.class)
                .withPropertyValues(
                        "kurin.node.port=" + TestPorts.freePort(),
                        "kurin.node.advertised-host=127.0.0.1",
                        // A peer that does not exist: no quorum, no leader, ever
                        "kurin.node.peers=127.0.0.1:" + TestPorts.freePort(),
                        "kurin.node.storage=memory",
                        "kurin.node.heartbeat-interval=30ms",
                        "kurin.node.election-timeout-min=300ms",
                        "kurin.node.election-timeout-max=600ms",
                        "kurin.node.health.no-leader-threshold=500ms")
                .run(context -> {
                    KurinNode node = context.getBean(KurinNode.class);
                    Duration threshold = context.getBean(org.kurin.kurinspringbootstarter.starter.KurinProperties.class).getHealth().getNoLeaderThreshold();
                    assertThat(threshold).isEqualTo(Duration.ofMillis(500));
                    assertThat(KurinNodeHealth.of(node, threshold).status()).isNotEqualTo(Status.UP);
                    await().atMost(Duration.ofSeconds(10)).until(() -> KurinNodeHealth.of(node, threshold).status() == Status.OUT_OF_SERVICE);
                    assertThat(KurinNodeHealth.of(node, threshold).details()).containsKey("leaderUnknownForMs");
                });
    }
}
