package org.kurin.kurinlocksspringbootstarter;

import org.junit.jupiter.api.Test;
import org.kurin.api.exception.KurinNotLeaderException;
import org.kurin.kurinlocks.api.KurinLockContext;
import org.kurin.kurinlocks.api.KurinLockManager;
import org.kurin.kurinlocks.api.LockHandle;
import org.kurin.kurinlocks.exception.KurinLockAcquisitionException;
import org.kurin.kurinlocksspringbootstarter.annotation.KurinDistributedLock;
import org.kurin.kurinlocksspringbootstarter.annotation.KurinDistributedLock.OnBusy;
import org.kurin.kurinlocksspringbootstarter.annotation.KurinLeaderOnly;
import org.kurin.kurinlocksspringbootstarter.annotation.KurinLeaderOnly.OnNotLeader;
import org.kurin.kurinlocksspringbootstarter.template.KurinLockTemplate;
import org.kurin.kurinspringbootstarter.starter.KurinAutoConfiguration;
import org.kurin.raft.KurinNode;
import org.kurin.test.TestPorts;
import org.springframework.aop.aspectj.AspectJExpressionPointcut;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.autoconfigure.aop.AopAutoConfiguration;
import org.springframework.boot.test.context.assertj.AssertableApplicationContext;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.boot.test.context.runner.ContextConsumer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

class CoordinationAnnotationsTest {

    private ApplicationContextRunner runner() {
        return new ApplicationContextRunner()
                .withConfiguration(AutoConfigurations.of(KurinAutoConfiguration.class, KurinLocksAutoConfiguration.class, AopAutoConfiguration.class))
                .withPropertyValues(
                        "kurin.node.port=" + TestPorts.freePort(),
                        "kurin.node.storage=memory",
                        "kurin.node.heartbeat-interval=30ms",
                        "kurin.node.election-timeout-min=300ms",
                        "kurin.node.election-timeout-max=600ms");
    }

    private static ContextConsumer<AssertableApplicationContext> withLeader(ContextConsumer<AssertableApplicationContext> test) {
        return context -> {
            assertThat(context).hasNotFailed();
            KurinNode node = context.getBean(KurinNode.class);
            await().atMost(Duration.ofSeconds(10)).until(() -> node.leadership().isPresent());
            test.accept(context);
        };
    }

    public static class Orders {
        private final AtomicInteger executions = new AtomicInteger();

        // Read through a method: fields of the AOP proxy itself are not initialized
        public int executions() {
            return executions.get();
        }

        @KurinDistributedLock(key = "'order:' + #orderId")
        public String skipWhenBusy(String orderId) {
            executions.incrementAndGet();
            return "done:" + KurinLockContext.current().key();
        }

        @KurinDistributedLock(key = "'order:' + #orderId")
        public Optional<String> skipWhenBusyOptional(String orderId) {
            executions.incrementAndGet();
            return Optional.of("done");
        }

        @KurinDistributedLock(key = "'order:' + #orderId", onBusy = OnBusy.THROW)
        public String throwWhenBusy(String orderId) {
            executions.incrementAndGet();
            return "done";
        }

        @KurinDistributedLock(key = "'order:' + #orderId", onBusy = OnBusy.WAIT, waitTimeout = "200ms")
        public String waitWhenBusy(String orderId) {
            executions.incrementAndGet();
            return "done";
        }

        @KurinDistributedLock(key = "'order:' + #a0 + ':' + #root.methodName", onBusy = OnBusy.THROW)
        public long tokenOf(String orderId) {
            return KurinLockContext.current().fencingToken();
        }

        @KurinDistributedLock(key = "#order.id", onBusy = OnBusy.THROW)
        public String keyFromMissingProperty(String order) {
            executions.incrementAndGet();
            return "done";
        }

        @KurinDistributedLock(key = "#missing", onBusy = OnBusy.THROW)
        public String keyThatIsNull(String orderId) {
            executions.incrementAndGet();
            return "done";
        }

        @KurinDistributedLock(key = "'nested'", onBusy = OnBusy.THROW, reentrant = true)
        public long reentrantOuter(Orders self) {
            long outer = KurinLockContext.current().fencingToken();
            assertThat(self.reentrantInner()).isEqualTo(outer);
            // The inner call released its level only: the lock is still ours
            assertThat(KurinLockContext.current().isHeld()).isTrue();
            return outer;
        }

        @KurinDistributedLock(key = "'nested'", onBusy = OnBusy.THROW, reentrant = true)
        public long reentrantInner() {
            return KurinLockContext.current().fencingToken();
        }

        @KurinLeaderOnly(onNotLeader = OnNotLeader.THROW)
        public String leaderOrThrow() {
            return "led";
        }

        @KurinLeaderOnly
        public Optional<String> leaderOrEmpty() {
            return Optional.of("led");
        }
    }

    @Configuration(proxyBeanMethods = false)
    static class OrdersConfig {
        @Bean
        Orders orders() {
            return new Orders();
        }
    }

    @Test
    void lockAnnotationAppliesItsBusyPolicies() {
        runner().withUserConfiguration(OrdersConfig.class).run(withLeader(context -> {
            Orders orders = context.getBean(Orders.class);
            KurinLockManager manager = context.getBean(KurinLockManager.class);

            assertThat(orders.skipWhenBusy("7")).isEqualTo("done:order:7");
            assertThat(orders.executions()).isEqualTo(1);

            try (LockHandle heldElsewhere = manager.lock("order:7", Duration.ofSeconds(30), Duration.ZERO)) {
                assertThat(orders.skipWhenBusy("7")).isNull();
                assertThat(orders.skipWhenBusyOptional("7")).isEmpty();
                assertThatThrownBy(() -> orders.throwWhenBusy("7"))
                        .isInstanceOf(KurinLockAcquisitionException.class)
                        .hasMessageContaining("order:7");
                assertThatThrownBy(() -> orders.waitWhenBusy("7"))
                        .isInstanceOf(KurinLockAcquisitionException.class)
                        .hasMessageContaining("after waiting 200 ms");
                // Another key is not affected
                assertThat(orders.throwWhenBusy("8")).isEqualTo("done");
                assertThat(heldElsewhere.isHeld()).isTrue();
            }
            assertThat(orders.executions()).isEqualTo(2);

            // The method released its lock: it can run again, and each grant has a larger token
            long first = orders.tokenOf("9");
            long second = orders.tokenOf("9");
            assertThat(second).isGreaterThan(first);
        }));
    }

    @Test
    void failingKeyExpressionIsAnErrorNotALiteralKey() {
        runner().withUserConfiguration(OrdersConfig.class).run(withLeader(context -> {
            Orders orders = context.getBean(Orders.class);

            assertThatThrownBy(() -> orders.keyFromMissingProperty("x"))
                    .isInstanceOf(KurinLockKeyException.class)
                    .hasMessageContaining("#order.id")
                    .hasMessageContaining("Orders.keyFromMissingProperty");
            assertThatThrownBy(() -> orders.keyThatIsNull("x"))
                    .isInstanceOf(KurinLockKeyException.class)
                    .hasMessageContaining("evaluated to null");
            assertThat(orders.executions()).isZero();
        }));
    }

    @Test
    void reentrantLockLetsTheSameThreadNest() {
        runner().withUserConfiguration(OrdersConfig.class).run(withLeader(context -> {
            Orders orders = context.getBean(Orders.class);
            KurinLockManager manager = context.getBean(KurinLockManager.class);

            long token = orders.reentrantOuter(orders);

            assertThat(token).isPositive();
            assertThat(manager.tryLock("nested", Duration.ofSeconds(5))).isPresent();
        }));
    }

    @Test
    void leaderOnlyRunsOnTheLeaderAndTemplateWorks() {
        runner().withUserConfiguration(OrdersConfig.class).run(withLeader(context -> {
            Orders orders = context.getBean(Orders.class);
            KurinLockTemplate template = context.getBean(KurinLockTemplate.class);

            assertThat(orders.leaderOrThrow()).isEqualTo("led");
            assertThat(orders.leaderOrEmpty()).contains("led");

            assertThat(template.executeWithLock("report", Duration.ofSeconds(10), () -> "ran")).isEqualTo("ran");
            assertThat(template.tryExecuteWithLock("report", Duration.ofSeconds(10), () ->
                    template.tryExecuteWithLock("report", Duration.ofSeconds(10), () -> "inner"))).contains(Optional.empty());
        }));
    }

    @Test
    void leaderOnlyRefusesBeforeLeadershipIsEstablished() {
        // A node whose only peer does not exist can never become leader
        runner().withUserConfiguration(OrdersConfig.class)
                .withPropertyValues("kurin.node.advertised-host=127.0.0.1", "kurin.node.peers=127.0.0.1:" + TestPorts.freePort())
                .run(context -> {
                    assertThat(context).hasNotFailed();
                    Orders orders = context.getBean(Orders.class);

                    assertThat(orders.leaderOrEmpty()).isEmpty();
                    assertThatThrownBy(orders::leaderOrThrow).isInstanceOf(KurinNotLeaderException.class);
                });
    }

    // ---------------------------------------------------------------- startup validation

    public static class SkipOnPrimitive {
        @KurinDistributedLock(key = "'k'")
        public int count() {
            return 1;
        }
    }

    public static class LeaderOnlyPrimitive {
        @KurinLeaderOnly
        public boolean check() {
            return true;
        }
    }

    public static class WaitWithoutTimeout {
        @KurinDistributedLock(key = "'k'", onBusy = OnBusy.WAIT)
        public void run() {
        }
    }

    public static class UnquotedKey {
        @KurinDistributedLock(key = "nightly report")
        public void run() {
        }
    }

    public static class BadDuration {
        @KurinDistributedLock(key = "'k'", ttl = "soon")
        public void run() {
        }
    }

    private void assertStartupFails(Class<?> beanType, String... expectedFragments) {
        runner().withBean(beanType).run(context -> {
            assertThat(context).hasFailed();
            Throwable root = context.getStartupFailure();
            while (root.getCause() != null && !(root instanceof IllegalStateException && root.getMessage().contains("@Kurin"))) {
                root = root.getCause();
            }
            assertThat(root.getMessage()).contains(expectedFragments);
        });
    }

    @Test
    void misusedAnnotationsFailTheStartup() {
        assertStartupFails(SkipOnPrimitive.class, "onBusy = SKIP", "SkipOnPrimitive.count", "primitive type 'int'");
        assertStartupFails(LeaderOnlyPrimitive.class, "onNotLeader = SKIP", "primitive type 'boolean'");
        assertStartupFails(WaitWithoutTimeout.class, "onBusy = WAIT", "positive waitTimeout");
        assertStartupFails(UnquotedKey.class, "invalid key expression", "must be quoted");
        assertStartupFails(BadDuration.class, "invalid ttl 'soon'");
    }

    @Test
    void aspectPointcutsCompile() {
        // Guards against a typo in a pointcut expression, which Spring reports only when a bean is matched
        AspectJExpressionPointcut pointcut = new AspectJExpressionPointcut();
        pointcut.setExpression("@annotation(org.kurin.kurinlocksspringbootstarter.annotation.KurinScheduledOnce)");
        assertThat(pointcut.getClassFilter().matches(Orders.class)).isTrue();
    }
}
