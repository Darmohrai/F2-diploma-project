package org.kurin.kurinspringbootstarter;

import org.junit.jupiter.api.Test;
import org.kurin.api.KurinCommand;
import org.kurin.api.KurinRestore;
import org.kurin.api.KurinSnapshot;
import org.kurin.api.exception.KurinNotStartedException;
import org.kurin.kurinspringbootstarter.starter.KurinAutoConfiguration;
import org.kurin.kurinspringbootstarter.template.KurinTemplate;
import org.kurin.raft.KurinNode;
import org.kurin.test.TestPorts;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class KurinStartupValidationTest {

    private ApplicationContextRunner runner() {
        return new ApplicationContextRunner()
                .withConfiguration(AutoConfigurations.of(KurinAutoConfiguration.class))
                .withPropertyValues(
                        "kurin.node.port=" + TestPorts.freePort(),
                        "kurin.node.storage=memory",
                        "kurin.node.heartbeat-interval=30ms",
                        "kurin.node.election-timeout-min=300ms",
                        "kurin.node.election-timeout-max=600ms");
    }

    public record Ping(String text) {
    }

    public static class BrokenSnapshotService {
        @KurinCommand
        public String ping(Ping command) {
            return command.text();
        }

        @KurinSnapshot
        public String snapshot() {
            return "state";
        }

        @KurinRestore
        public void restore(byte[] data) {
        }
    }

    @Configuration(proxyBeanMethods = false)
    static class BrokenSnapshotConfig {
        @Bean
        BrokenSnapshotService brokenSnapshotService() {
            return new BrokenSnapshotService();
        }
    }

    // Spec test 32
    @Test
    void wrongSnapshotSignatureFailsContextStartupWithClearMessage() {
        runner().withUserConfiguration(BrokenSnapshotConfig.class).run(context -> {
            assertThat(context).hasFailed();
            assertThat(context.getStartupFailure())
                    .rootCause()
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("@KurinSnapshot")
                    .hasMessageContaining("BrokenSnapshotService.snapshot")
                    .hasMessageContaining("return byte[]");
        });
    }

    public static class TwoParameterService {
        @KurinCommand
        public String ping(Ping command, String extra) {
            return command.text();
        }
    }

    @Configuration(proxyBeanMethods = false)
    static class TwoParameterConfig {
        @Bean
        TwoParameterService twoParameterService() {
            return new TwoParameterService();
        }
    }

    @Test
    void wrongCommandSignatureFailsContextStartup() {
        runner().withUserConfiguration(TwoParameterConfig.class).run(context -> {
            assertThat(context).hasFailed();
            assertThat(context.getStartupFailure()).rootCause()
                    .hasMessageContaining("@KurinCommand")
                    .hasMessageContaining("exactly one parameter");
        });
    }

    @Test
    void invalidTimingConfigurationFailsContextStartup() {
        runner().withUserConfiguration(PingConfig.class)
                .withPropertyValues("kurin.node.heartbeat-interval=200ms", "kurin.node.election-timeout-min=500ms")
                .run(context -> {
                    assertThat(context).hasFailed();
                    assertThat(context.getStartupFailure()).rootCause()
                            .hasMessageContaining("electionTimeoutMinMs")
                            .hasMessageContaining("5 * heartbeatIntervalMs");
                });
    }

    @org.kurin.api.KurinService(stateless = true)
    public static class PingService {
        @KurinCommand
        public String ping(Ping command) {
            return command.text();
        }
    }

    /**
     * Uses the template while the context is still being built, before the node is started.
     */
    public record Remember(String text) {
    }

    /**
     * Keeps what its commands bring, but cannot hand it to a node that catches up from a snapshot.
     */
    public static class ForgetfulService {
        private final java.util.List<String> remembered = new java.util.ArrayList<>();

        @KurinCommand
        public int remember(Remember command) {
            remembered.add(command.text());
            return remembered.size();
        }
    }

    @Configuration(proxyBeanMethods = false)
    static class ForgetfulConfig {
        @Bean
        ForgetfulService forgetfulService() {
            return new ForgetfulService();
        }
    }

    @Test
    void serviceWithCommandsAndNoSnapshotFailsContextStartup() {
        runner().withUserConfiguration(ForgetfulConfig.class).run(context -> {
            assertThat(context).hasFailed();
            assertThat(context.getStartupFailure()).rootCause()
                    .hasMessageContaining(ForgetfulService.class.getName())
                    .hasMessageContaining("@KurinSnapshot")
                    .hasMessageContaining("stateless = true")
                    .hasMessageContaining("kurin.node.allow-services-without-snapshot");
        });
    }

    @Test
    void serviceWithoutSnapshotStartsWhenExplicitlyAllowed() {
        runner().withUserConfiguration(ForgetfulConfig.class)
                .withPropertyValues("kurin.node.allow-services-without-snapshot=true")
                .run(context -> {
                    assertThat(context).hasNotFailed();
                    KurinNode node = context.getBean(KurinNode.class);
                    org.awaitility.Awaitility.await().atMost(java.time.Duration.ofSeconds(10)).until(() -> node.leadership().isPresent());
                    assertThat(context.getBean(KurinTemplate.class).<Integer>submitCommand(new Remember("a")).get(5, TimeUnit.SECONDS))
                            .isEqualTo(1);
                });
    }

    @org.kurin.api.KurinService(stateless = true)
    public static class ContradictoryService {
        @KurinCommand
        public String ping(Ping command) {
            return command.text();
        }

        @KurinSnapshot
        public byte[] snapshot() {
            return new byte[0];
        }

        @KurinRestore
        public void restore(byte[] snapshot) {
            // Nothing is kept, so there is nothing to put back
        }
    }

    @Configuration(proxyBeanMethods = false)
    static class ContradictoryConfig {
        @Bean
        ContradictoryService contradictoryService() {
            return new ContradictoryService();
        }
    }

    @Test
    void statelessServiceWithSnapshotMethodsFailsContextStartup() {
        runner().withUserConfiguration(ContradictoryConfig.class).run(context -> {
            assertThat(context).hasFailed();
            assertThat(context.getStartupFailure()).rootCause()
                    .hasMessageContaining("declared stateless")
                    .hasMessageContaining("@KurinSnapshot");
        });
    }

    public static class EagerCaller implements InitializingBean {
        static final AtomicReference<CompletableFuture<Object>> EARLY_RESULT = new AtomicReference<>();
        private final KurinTemplate kurinTemplate;

        public EagerCaller(KurinTemplate kurinTemplate) {
            this.kurinTemplate = kurinTemplate;
        }

        @Override
        public void afterPropertiesSet() {
            EARLY_RESULT.set(kurinTemplate.submitCommand(new Ping("too early")));
        }
    }

    @Configuration(proxyBeanMethods = false)
    static class PingConfig {
        @Bean
        PingService pingService() {
            return new PingService();
        }

        @Bean
        EagerCaller eagerCaller(KurinTemplate kurinTemplate) {
            return new EagerCaller(kurinTemplate);
        }
    }

    @Test
    void commandSubmittedBeforeStartFailsWithNotStartedAndNodeStartsAfterAllBeansExist() {
        runner().withUserConfiguration(PingConfig.class).run(context -> {
            assertThat(context).hasNotFailed();
            assertThatThrownBy(() -> EagerCaller.EARLY_RESULT.get().get(1, TimeUnit.SECONDS))
                    .isInstanceOf(ExecutionException.class)
                    .hasCauseInstanceOf(KurinNotStartedException.class);

            KurinNode node = context.getBean(KurinNode.class);
            assertThat(node.isStarted()).isTrue();
            org.awaitility.Awaitility.await().atMost(java.time.Duration.ofSeconds(10)).until(() -> node.leadership().isPresent());
            assertThat(context.getBean(KurinTemplate.class).<String>submitCommand(new Ping("now")).get(5, TimeUnit.SECONDS))
                    .isEqualTo("now");
        });
    }
}
