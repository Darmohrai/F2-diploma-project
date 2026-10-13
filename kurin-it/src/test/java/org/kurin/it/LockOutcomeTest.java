package org.kurin.it;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.kurin.api.exception.KurinException;
import org.kurin.kurinlocks.api.KurinLockManager;
import org.kurin.kurinlocks.api.LockHandle;
import org.kurin.kurinlocks.exception.KurinLockAcquisitionException;
import org.kurin.raft.KurinNode;
import org.kurin.test.TestPorts;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;

import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

/**
 * Two instances of the application. A lock that another instance holds is "busy"; a cluster that cannot answer
 * is not: then the caller gets an exception, with every busy policy.
 */
class LockOutcomeTest {
    private static final Duration SOON = Duration.ofSeconds(20);
    private static final Duration TTL = Duration.ofSeconds(30);

    @TempDir
    Path directory;

    private final List<ConfigurableApplicationContext> instances = new ArrayList<>();

    @BeforeEach
    void startTwoInstances() {
        Shared.POLICY_RUNS.set(0);
        String databaseId = "it-" + UUID.randomUUID();
        List<Integer> ports = TestPorts.freePorts(2);
        String peers = ports.stream().map(port -> "127.0.0.1:" + port).collect(Collectors.joining(","));
        for (int i = 0; i < ports.size(); i++) {
            instances.add(new SpringApplicationBuilder(ItApplication.class)
                    .properties(
                            "server.port=0",
                            "spring.jmx.enabled=false",
                            "spring.main.banner-mode=off",
                            "it.database-id=" + databaseId,
                            "kurin.node.port=" + ports.get(i),
                            "kurin.node.peers=" + peers,
                            "kurin.locks.command-timeout=2s",
                            "kurin.cache.data-dir=" + directory.resolve("instance-" + i).resolve("cache"))
                    .run());
        }
        await("a leader known to both instances").atMost(SOON).until(() -> {
            long leaders = instances.stream().filter(context -> node(context).leadership().isPresent()).count();
            String leaderId = node(instances.get(0)).status().leaderId();
            return leaders == 1 && leaderId != null && leaderId.equals(node(instances.get(1)).status().leaderId());
        });
        // Nothing more is waited for: the instance that started first found its peer away at first, and its
        // requests must get through all the same as soon as it follows the leader
    }

    @AfterEach
    void stopInstances() {
        instances.forEach(ConfigurableApplicationContext::close);
    }

    private static KurinNode node(ConfigurableApplicationContext context) {
        return context.getBean(KurinNode.class);
    }

    private ConfigurableApplicationContext leader() {
        return instances.stream().filter(context -> node(context).leadership().isPresent()).findFirst().orElseThrow();
    }

    private ConfigurableApplicationContext follower() {
        ConfigurableApplicationContext leader = leader();
        return instances.stream().filter(context -> context != leader).findFirst().orElseThrow();
    }

    @Test
    void lockHeldByTheOtherInstanceIsBusy() {
        ConfigurableApplicationContext follower = follower();
        KurinLockManager locks = follower.getBean(KurinLockManager.class);
        Coordination coordination = follower.getBean(Coordination.class);

        try (LockHandle heldElsewhere = leader().getBean(KurinLockManager.class).lock("it-policy", TTL, Duration.ZERO)) {
            assertThat(locks.tryLock("it-policy", TTL)).isEmpty();
            assertThat(coordination.skipIfBusy()).isNull();
            assertThatThrownBy(coordination::throwIfBusy).isExactlyInstanceOf(KurinLockAcquisitionException.class);
            assertThatThrownBy(coordination::waitIfBusy).isExactlyInstanceOf(KurinLockAcquisitionException.class);
            assertThat(heldElsewhere.isHeld()).isTrue();
        }
        assertThat(Shared.POLICY_RUNS.get()).isZero();

        // Released: the same calls go through
        assertThat(coordination.skipIfBusy()).isEqualTo("ran");
        assertThat(coordination.throwIfBusy()).isEqualTo("ran");
        assertThat(coordination.waitIfBusy()).isEqualTo("ran");
    }

    @Test
    void commandFromTheFollowerWhileTheLeaderIsOutOfReachIsAnErrorNotABusyLock() {
        ConfigurableApplicationContext follower = follower();
        KurinLockManager locks = follower.getBean(KurinLockManager.class);
        Coordination coordination = follower.getBean(Coordination.class);

        // The leader goes away. One node of two cannot elect a leader, so nobody can answer for the lock.
        leader().close();

        assertThatThrownBy(() -> locks.tryLock("it-policy", TTL))
                .isInstanceOf(KurinException.class)
                .isNotInstanceOf(KurinLockAcquisitionException.class)
                .hasMessageContaining("it-policy");
        assertThatThrownBy(coordination::skipIfBusy)
                .isInstanceOf(KurinException.class).isNotInstanceOf(KurinLockAcquisitionException.class);
        assertThatThrownBy(coordination::throwIfBusy)
                .isInstanceOf(KurinException.class).isNotInstanceOf(KurinLockAcquisitionException.class);
        assertThatThrownBy(coordination::waitIfBusy)
                .isInstanceOf(KurinException.class).isNotInstanceOf(KurinLockAcquisitionException.class);
        assertThat(Shared.POLICY_RUNS.get()).isZero();
    }
}
