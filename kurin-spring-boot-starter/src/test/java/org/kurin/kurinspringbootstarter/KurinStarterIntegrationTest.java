package org.kurin.kurinspringbootstarter;

import org.junit.jupiter.api.Test;
import org.kurin.api.KurinCommand;
import org.kurin.api.KurinQuery;
import org.kurin.api.ReadConsistency;
import org.kurin.api.exception.KurinCommandFailedException;
import org.kurin.kurinspringbootstarter.events.KurinLeaderElectedEvent;
import org.kurin.kurinspringbootstarter.template.KurinTemplate;
import org.kurin.raft.KurinNode;
import org.kurin.test.TestPorts;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Bean;
import org.springframework.context.event.EventListener;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

@SpringBootTest(classes = KurinStarterIntegrationTest.TestConfig.class, properties = {
        "kurin.node.storage=memory",
        "kurin.node.heartbeat-interval=30ms",
        "kurin.node.election-timeout-min=300ms",
        "kurin.node.election-timeout-max=600ms",
        "kurin.serialization.types=org.kurin.kurinspringbootstarter.KurinStarterIntegrationTest$Tag"
})
class KurinStarterIntegrationTest {

    @DynamicPropertySource
    static void nodePort(DynamicPropertyRegistry registry) {
        registry.add("kurin.node.port", TestPorts::freePort);
    }

    @EnableAutoConfiguration
    static class TestConfig {
        @Bean
        BankService bankService(KurinTemplate kurinTemplate) {
            return new BankService(kurinTemplate);
        }

        @Bean
        LeadershipRecorder leadershipRecorder() {
            return new LeadershipRecorder();
        }
    }

    public record DepositCommand(String accountId, double amount, Object label) {
    }

    public record CloseAccount(String accountId) {
    }

    public record BalanceQuery(String accountId) {
    }

    /**
     * Reachable only through the Object-typed field of DepositCommand, hence registered by property.
     */
    public record Tag(String value) {
    }

    /**
     * A state machine service that depends on KurinTemplate: with 1.0.x this was a dependency cycle
     * that silently left the service unregistered.
     */
    public static class BankService {
        private final KurinTemplate kurinTemplate;
        private final Map<String, Double> accounts = new HashMap<>();

        public BankService(KurinTemplate kurinTemplate) {
            this.kurinTemplate = kurinTemplate;
        }

        public double deposit(String accountId, double amount) throws Exception {
            return kurinTemplate.<Double>submitCommand(new DepositCommand(accountId, amount, new Tag("manual"))).get(5, TimeUnit.SECONDS);
        }

        @KurinCommand
        public double onDeposit(DepositCommand command) {
            return accounts.merge(command.accountId(), command.amount(), Double::sum);
        }

        @KurinCommand
        public void onClose(CloseAccount command) {
            if (accounts.remove(command.accountId()) == null) {
                throw new IllegalArgumentException("No such account: " + command.accountId());
            }
        }

        @KurinQuery
        public double balance(BalanceQuery query) {
            return accounts.getOrDefault(query.accountId(), 0.0);
        }
    }

    public static class LeadershipRecorder {
        final List<KurinLeaderElectedEvent> elected = new CopyOnWriteArrayList<>();
        final List<String> threads = new CopyOnWriteArrayList<>();

        @EventListener
        public void onElected(KurinLeaderElectedEvent event) {
            elected.add(event);
            threads.add(Thread.currentThread().getName());
        }
    }

    @Autowired
    private KurinNode kurinNode;

    @Autowired
    private KurinTemplate kurinTemplate;

    @Autowired
    private BankService bankService;

    @Autowired
    private LeadershipRecorder leadershipRecorder;

    private void awaitLeadership() {
        await().atMost(Duration.ofSeconds(10)).until(() -> kurinTemplate.leadership().isPresent());
    }

    // Spec test 31
    @Test
    void serviceInjectingKurinTemplateIsRegisteredAndItsCommandsRun() throws Exception {
        awaitLeadership();

        assertThat(bankService.deposit("ACC-1", 1000.0)).isEqualTo(1000.0);
        assertThat(bankService.deposit("ACC-1", 250.0)).isEqualTo(1250.0);

        double balance = kurinTemplate.<Double>query(new BalanceQuery("ACC-1")).get(5, TimeUnit.SECONDS);
        double leaseBalance = kurinTemplate.<Double>query(new BalanceQuery("ACC-1"), ReadConsistency.LEASE).get(5, TimeUnit.SECONDS);
        assertThat(balance).isEqualTo(1250.0);
        assertThat(leaseBalance).isEqualTo(1250.0);
    }

    @Test
    void voidCommandAndHandlerFailureReachTheCaller() throws Exception {
        awaitLeadership();
        bankService.deposit("ACC-2", 10.0);

        assertThat(kurinTemplate.submitCommand(new CloseAccount("ACC-2")).get(5, TimeUnit.SECONDS)).isNull();
        assertThatThrownBy(() -> kurinTemplate.submitCommand(new CloseAccount("ACC-2")).get(5, TimeUnit.SECONDS))
                .isInstanceOf(ExecutionException.class)
                .cause()
                .isInstanceOfSatisfying(KurinCommandFailedException.class, failure -> {
                    assertThat(failure.causeType()).isEqualTo(IllegalArgumentException.class.getName());
                    assertThat(failure.causeMessage()).isEqualTo("No such account: ACC-2");
                });
    }

    @Test
    void leadershipEventIsPublishedOffTheRaftThread() {
        awaitLeadership();

        await().atMost(Duration.ofSeconds(5)).until(() -> !leadershipRecorder.elected.isEmpty());
        KurinLeaderElectedEvent event = leadershipRecorder.elected.get(0);
        assertThat(event.getNodeId()).isEqualTo(kurinNode.nodeId());
        assertThat(event.getLeaderId()).isEqualTo(kurinNode.nodeId());
        assertThat(event.getTerm()).isEqualTo(kurinTemplate.status().term());
        assertThat(leadershipRecorder.threads).containsOnly("kurin-events");
    }
}
