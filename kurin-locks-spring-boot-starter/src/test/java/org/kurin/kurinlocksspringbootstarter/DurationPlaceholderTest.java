package org.kurin.kurinlocksspringbootstarter;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.kurin.kurinlocks.api.KurinLockContext;
import org.kurin.kurinlocks.api.KurinLockManager;
import org.kurin.kurinlocks.api.LockHandle;
import org.kurin.kurinlocks.exception.KurinLockAcquisitionException;
import org.kurin.kurinlocks.job.JobCommands.JobMode;
import org.kurin.kurinlocks.job.KurinJobContext;
import org.kurin.kurinlocks.job.KurinJobs;
import org.kurin.kurinlocksspringbootstarter.KurinCoordinationRegistry.LeaderTaskMethod;
import org.kurin.kurinlocksspringbootstarter.annotation.KurinDistributedLock;
import org.kurin.kurinlocksspringbootstarter.annotation.KurinDistributedLock.OnBusy;
import org.kurin.kurinlocksspringbootstarter.annotation.KurinLeaderTask;
import org.kurin.kurinlocksspringbootstarter.annotation.KurinScheduledOnce;
import org.kurin.raft.KurinNode;
import org.kurin.test.TestPorts;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;

import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

/**
 * 1.1.2: the duration attributes of the coordination annotations take their values from the configuration.
 * The applications are started the way Spring Boot starts them, so the placeholders are resolved from
 * {@code application-durations.properties} like any other property.
 */
class DurationPlaceholderTest {

    public static class Tasks {
        private final AtomicInteger housekeepingRuns = new AtomicInteger();

        // Read through a method: fields of the AOP proxy itself are not initialized
        public int housekeepingRuns() {
            return housekeepingRuns.get();
        }

        @KurinLeaderTask(name = "from-properties", interval = "${app.housekeeping.interval}")
        public void housekeeping() {
            housekeepingRuns.incrementAndGet();
        }

        @KurinLeaderTask(name = "with-default", interval = "${app.absent.interval:10s}")
        public void withDefault() {
        }

        @KurinLeaderTask(name = "milliseconds", interval = "${app.millis.interval}")
        public void milliseconds() {
        }

        @KurinLeaderTask(name = "iso", interval = "PT2M")
        public void iso() {
        }

        @KurinLeaderTask(name = "simple", interval = "3h")
        public void simple() {
        }

        @KurinLeaderTask(name = "plain-number", interval = "1500")
        public void plainNumber() {
        }

        @KurinDistributedLock(key = "'configured'", ttl = "${app.lock.ttl}", waitTimeout = "${app.lock.wait}", onBusy = OnBusy.WAIT)
        public Instant lockedUntil() {
            return KurinLockContext.current().expiresAt();
        }

        @KurinScheduledOnce(name = "configured-job", slot = "${app.job.slot}", mode = JobMode.AT_LEAST_ONCE, lease = "${app.job.lease:1m}")
        public Long slot() {
            return KurinJobContext.current().slot();
        }
    }

    public static class UnsetInterval {
        @KurinLeaderTask(name = "unset", interval = "${app.not.configured}")
        public void run() {
        }
    }

    public static class InvalidInterval {
        @KurinLeaderTask(name = "invalid", interval = "${app.invalid.interval}")
        public void run() {
        }
    }

    public static class NegativeInterval {
        @KurinLeaderTask(name = "negative", interval = "${app.negative.interval}")
        public void run() {
        }
    }

    public static class UnsetLockTtl {
        @KurinDistributedLock(key = "'k'", ttl = "${app.not.configured}")
        public void run() {
        }
    }

    public static class InvalidWaitTimeout {
        @KurinDistributedLock(key = "'k'", waitTimeout = "${app.invalid.interval}")
        public void run() {
        }
    }

    public static class UnsetSlot {
        @KurinScheduledOnce(name = "job", slot = "${app.not.configured}")
        public void run() {
        }
    }

    public static class InvalidLease {
        @KurinScheduledOnce(name = "job", lease = "${app.invalid.interval}")
        public void run() {
        }
    }

    @EnableAutoConfiguration
    static class App {
    }

    private ConfigurableApplicationContext context;

    @AfterEach
    void stop() {
        if (context != null) {
            context.close();
        }
    }

    private ConfigurableApplicationContext start(Class<?> beanType, String... properties) {
        context = new SpringApplicationBuilder(App.class, beanType)
                .web(WebApplicationType.NONE)
                .profiles("durations")
                .properties("kurin.node.port=" + TestPorts.freePort(),
                        "kurin.node.storage=memory",
                        "kurin.node.heartbeat-interval=30ms",
                        "kurin.node.election-timeout-min=300ms",
                        "kurin.node.election-timeout-max=600ms",
                        "spring.main.banner-mode=off")
                .properties(properties)
                .run();
        return context;
    }

    private static Throwable rootCause(Throwable error) {
        Throwable root = error;
        while (root.getCause() != null && !(root instanceof IllegalStateException && root.getMessage().startsWith("@Kurin"))) {
            root = root.getCause();
        }
        return root;
    }

    private void assertStartupFails(Class<?> beanType, String[] properties, String... expectedFragments) {
        assertThatThrownBy(() -> start(beanType, properties))
                .satisfies(error -> assertThat(rootCause(error)).isInstanceOf(IllegalStateException.class)
                        .hasMessageContainingAll(expectedFragments));
    }

    @Test
    void intervalComesFromTheConfigurationInEveryFormat() {
        ConfigurableApplicationContext application = start(Tasks.class);
        Map<String, Duration> intervals = application.getBean(KurinCoordinationRegistry.class).leaderTasks().stream()
                .collect(Collectors.toMap(LeaderTaskMethod::name, LeaderTaskMethod::interval));

        assertThat(intervals).containsOnly(
                Map.entry("from-properties", Duration.ofMillis(25)),
                // The key is absent, so the default after the colon is used
                Map.entry("with-default", Duration.ofSeconds(10)),
                // A number without a unit is milliseconds, as in @Scheduled(fixedDelayString)
                Map.entry("milliseconds", Duration.ofMillis(40)),
                Map.entry("iso", Duration.ofMinutes(2)),
                Map.entry("simple", Duration.ofHours(3)),
                Map.entry("plain-number", Duration.ofMillis(1500)));

        // And the task really runs at that pace on the leader, which a single node is
        Tasks tasks = application.getBean(Tasks.class);
        await().atMost(Duration.ofSeconds(20)).until(() -> tasks.housekeepingRuns() >= 5);
    }

    @Test
    void lockAndJobAttributesComeFromTheConfigurationToo() {
        ConfigurableApplicationContext application = start(Tasks.class);
        KurinNode node = application.getBean(KurinNode.class);
        await().atMost(Duration.ofSeconds(10)).until(() -> node.leadership().isPresent());
        Tasks tasks = application.getBean(Tasks.class);

        // ttl = 7s, not the default of 30s
        Instant before = Instant.now();
        Instant lockedUntil = tasks.lockedUntil();
        assertThat(Duration.between(before, lockedUntil)).isBetween(Duration.ofSeconds(6), Duration.ofSeconds(7).plusMillis(999));

        // waitTimeout = 0.2s: a busy lock is waited for, for that long
        try (LockHandle held = application.getBean(KurinLockManager.class).lock("configured", Duration.ofSeconds(30), Duration.ZERO)) {
            assertThatThrownBy(tasks::lockedUntil)
                    .isInstanceOf(KurinLockAcquisitionException.class)
                    .hasMessageContaining("after waiting 200 ms");
            assertThat(held.isHeld()).isTrue();
        }

        // slot = 365 days
        assertThat(tasks.slot()).isEqualTo(KurinJobs.slotOf(System.currentTimeMillis(), Duration.ofDays(365)));
    }

    @Test
    void placeholderWithoutAValueStopsTheStartupAndNamesBeanMethodAndAttribute() {
        assertStartupFails(UnsetInterval.class, new String[0],
                "@KurinLeaderTask", "UnsetInterval.run", "bean '", "unresolvable interval '${app.not.configured}'", "app.not.configured");
        assertStartupFails(UnsetLockTtl.class, new String[0],
                "@KurinDistributedLock", "UnsetLockTtl.run", "unresolvable ttl '${app.not.configured}'");
        assertStartupFails(UnsetSlot.class, new String[0],
                "@KurinScheduledOnce", "UnsetSlot.run", "unresolvable slot '${app.not.configured}'");
    }

    @Test
    void valueThatIsNotADurationStopsTheStartupAndShowsWhatItResolvedTo() {
        String[] invalid = {"app.invalid.interval=every now and then"};
        assertStartupFails(InvalidInterval.class, invalid,
                "@KurinLeaderTask", "InvalidInterval.run", "bean '", "invalid interval '${app.invalid.interval}'",
                "resolves to 'every now and then'", "number of milliseconds");
        assertStartupFails(InvalidWaitTimeout.class, invalid,
                "@KurinDistributedLock", "InvalidWaitTimeout.run", "invalid waitTimeout '${app.invalid.interval}'");
        assertStartupFails(InvalidLease.class, invalid,
                "@KurinScheduledOnce", "InvalidLease.run", "invalid lease '${app.invalid.interval}'");
    }

    @Test
    void resolvedIntervalIsCheckedLikeAWrittenOne() {
        assertStartupFails(NegativeInterval.class, new String[]{"app.negative.interval=-5s"},
                "@KurinLeaderTask", "NegativeInterval.run", "interval of at least one millisecond");
    }

    @Test
    void registryOutsideOfAContextTakesValuesAsWritten() {
        KurinCoordinationRegistry registry = new KurinCoordinationRegistry();

        assertThat(registry.resolveDuration("45s")).isEqualTo(Duration.ofSeconds(45));
        assertThat(KurinCoordinationRegistry.parseDuration("PT1M")).isEqualTo(Duration.ofMinutes(1));
        assertThatThrownBy(() -> registry.resolveDuration("${app.lock.ttl}")).isInstanceOf(IllegalArgumentException.class);
    }
}
