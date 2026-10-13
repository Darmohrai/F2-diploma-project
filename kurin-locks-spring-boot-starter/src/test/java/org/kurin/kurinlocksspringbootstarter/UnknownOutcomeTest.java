package org.kurin.kurinlocksspringbootstarter;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.kurin.api.exception.KurinNoLeaderException;
import org.kurin.kurinlocks.api.KurinLockManager;
import org.kurin.kurinlocks.api.LockHandle;
import org.kurin.kurinlocks.exception.KurinLockAcquisitionException;
import org.kurin.kurinlocksspringbootstarter.annotation.KurinDistributedLock;
import org.kurin.kurinlocksspringbootstarter.annotation.KurinDistributedLock.OnBusy;
import org.kurin.kurinlocksspringbootstarter.annotation.KurinScheduledOnce;
import org.kurin.kurinlocksspringbootstarter.template.KurinLockTemplate;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;

import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

/**
 * 1.1.2: {@code onBusy} decides about a lock that is busy, and about nothing else. An instance that cannot reach
 * the cluster does not know whether the lock is busy: its method does not run and the caller is told why.
 */
class UnknownOutcomeTest {

    @EnableAutoConfiguration
    static class App {
        @Bean
        MeterRegistry meterRegistry() {
            return new SimpleMeterRegistry();
        }

        @Bean
        Ledger ledger() {
            return new Ledger();
        }
    }

    public static class Ledger {
        private final AtomicInteger executions = new AtomicInteger();

        // Read through a method: fields of the AOP proxy itself are not initialized
        public int executions() {
            return executions.get();
        }

        @KurinDistributedLock(key = "'ledger'", onBusy = OnBusy.SKIP)
        public String skip() {
            executions.incrementAndGet();
            return "done";
        }

        @KurinDistributedLock(key = "'ledger'", onBusy = OnBusy.SKIP)
        public Optional<String> skipOptional() {
            executions.incrementAndGet();
            return Optional.of("done");
        }

        @KurinDistributedLock(key = "'ledger'", onBusy = OnBusy.THROW)
        public String throwing() {
            executions.incrementAndGet();
            return "done";
        }

        @KurinDistributedLock(key = "'ledger'", onBusy = OnBusy.WAIT, waitTimeout = "300ms")
        public String waiting() {
            executions.incrementAndGet();
            return "done";
        }

        @KurinScheduledOnce(name = "ledger-report", slot = "P365D")
        public String report() {
            executions.incrementAndGet();
            return "done";
        }
    }

    private SpringCluster cluster;

    @BeforeEach
    void start() {
        cluster = new SpringCluster(3, App.class, "kurin.locks.command-timeout=2s");
        cluster.awaitLeader();
    }

    @AfterEach
    void stop() {
        cluster.close();
    }

    private ConfigurableApplicationContext follower() {
        ConfigurableApplicationContext leader = cluster.leader().orElseThrow();
        return cluster.contexts().stream().filter(context -> context != leader).findFirst().orElseThrow();
    }

    private static double count(ConfigurableApplicationContext context, String meter, String... tags) {
        Counter counter = context.getBean(MeterRegistry.class).find(meter).tags(tags).counter();
        return counter != null ? counter.count() : 0;
    }

    @Test
    void busyLockFollowsOnBusy() {
        ConfigurableApplicationContext instance = follower();
        Ledger ledger = instance.getBean(Ledger.class);
        KurinLockManager elsewhere = cluster.leader().orElseThrow().getBean(KurinLockManager.class);

        try (LockHandle held = elsewhere.lock("ledger", Duration.ofSeconds(30), Duration.ZERO)) {
            assertThat(ledger.skip()).isNull();
            assertThat(ledger.skipOptional()).isEmpty();
            assertThatThrownBy(ledger::throwing)
                    .isExactlyInstanceOf(KurinLockAcquisitionException.class)
                    .hasMessageContaining("held by another owner");
            assertThatThrownBy(ledger::waiting)
                    .isExactlyInstanceOf(KurinLockAcquisitionException.class)
                    .hasMessageContaining("after waiting 300 ms");
            assertThat(held.isHeld()).isTrue();
        }

        assertThat(ledger.executions()).isZero();
        assertThat(count(instance, "kurin.locks.acquire", "result", "busy")).isEqualTo(4);
        assertThat(count(instance, "kurin.locks.acquire", "result", "unknown")).isZero();
        // Free again: every policy runs the method
        assertThat(ledger.skip()).isEqualTo("done");
        assertThat(ledger.throwing()).isEqualTo("done");
        assertThat(ledger.waiting()).isEqualTo("done");
    }

    @Test
    void instanceCutOffFromTheClusterThrowsWhateverOnBusySays() {
        ConfigurableApplicationContext instance = follower();
        Ledger ledger = instance.getBean(Ledger.class);
        KurinLockTemplate template = instance.getBean(KurinLockTemplate.class);

        cluster.isolate(instance);
        await("the instance notices that it has no leader").atMost(Duration.ofSeconds(15))
                .until(() -> cluster.node(instance).status().leaderId() == null);

        // The lock is free. This instance cannot know that, and does not pretend it is busy.
        assertThatThrownBy(ledger::skip).isExactlyInstanceOf(KurinNoLeaderException.class).hasMessageContaining("Lock 'ledger'");
        assertThatThrownBy(ledger::skipOptional).isExactlyInstanceOf(KurinNoLeaderException.class);
        assertThatThrownBy(ledger::throwing).isExactlyInstanceOf(KurinNoLeaderException.class);
        assertThatThrownBy(ledger::waiting).isExactlyInstanceOf(KurinNoLeaderException.class);
        assertThatThrownBy(() -> template.executeWithLock("ledger", Duration.ofSeconds(30), () -> "ran"))
                .isExactlyInstanceOf(KurinNoLeaderException.class);
        assertThatThrownBy(() -> template.tryExecuteWithLock("ledger", Duration.ofSeconds(30), () -> "ran"))
                .isExactlyInstanceOf(KurinNoLeaderException.class);

        assertThat(ledger.executions()).isZero();
        assertThat(count(instance, "kurin.locks.acquire", "result", "unknown")).isEqualTo(6);
        assertThat(count(instance, "kurin.locks.acquire", "result", "busy")).isZero();

        cluster.heal();
        cluster.awaitLeader();
        assertThat(ledger.throwing()).isEqualTo("done");
        assertThat(ledger.executions()).isEqualTo(1);
    }

    @Test
    void scheduledOnceSkipsTheRunWhenTheClaimGetsNoAnswer() {
        ConfigurableApplicationContext instance = follower();
        Ledger ledger = instance.getBean(Ledger.class);

        cluster.isolate(instance);
        await().atMost(Duration.ofSeconds(15)).until(() -> cluster.node(instance).status().leaderId() == null);

        // A schedule has nobody to throw to: the run is skipped, logged and counted
        assertThat(ledger.report()).isNull();
        assertThat(ledger.executions()).isZero();
        assertThat(count(instance, "kurin.jobs.claims", "job", "ledger-report", "result", "unknown")).isEqualTo(1);
        assertThat(count(instance, "kurin.jobs.claims", "job", "ledger-report", "result", "denied")).isZero();

        cluster.heal();
        cluster.awaitLeader();
        assertThat(ledger.report()).isEqualTo("done");
        assertThat(count(instance, "kurin.jobs.claims", "job", "ledger-report", "result", "granted")).isEqualTo(1);
    }
}
