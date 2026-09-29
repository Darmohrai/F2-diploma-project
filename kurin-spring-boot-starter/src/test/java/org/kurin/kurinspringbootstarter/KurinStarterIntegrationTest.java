package org.kurin.kurinspringbootstarter;

import org.junit.jupiter.api.Test;
import org.kurin.api.KurinCommand;
import org.kurin.raft.KurinNode;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;

@SpringBootTest(properties = {
        "kurin.node.host=127.0.0.1",
        "kurin.node.port=9011",
        "kurin.node.peers="
})
class KurinStarterIntegrationTest {

    // 1. Явно оголошуємо тестовий конфіг і створюємо сервіс як бін
    @SpringBootApplication
    static class TestConfig {
        @Bean
        public BankService bankService() {
            return new BankService();
        }
    }

    public record DepositCommand(String accountId, double amount) {}

    public static class BankService {
        private final Map<String, Double> accounts = new HashMap<>();

        @KurinCommand
        public String deposit(DepositCommand cmd) {
            double newBalance = accounts.getOrDefault(cmd.accountId(), 0.0) + cmd.amount();
            accounts.put(cmd.accountId(), newBalance);
            return "SUCCESS_DEPOSIT";
        }

        public double getBalance(String accountId) {
            return accounts.getOrDefault(accountId, 0.0);
        }
    }

    @Autowired
    private KurinNode kurinNode;

    @Autowired
    private BankService bankService;

    @Test
    void shouldProcessCommandViaSpringRaft() throws Exception {
        Thread.sleep(1000); // Очікування виборів лідера (Quorum = 1)

        DepositCommand command = new DepositCommand("ACC_SPRING_1", 1000.0);
        CompletableFuture<Object> future = kurinNode.submitCommand(command);

        Object result = future.get(3, TimeUnit.SECONDS);

        assertEquals("SUCCESS_DEPOSIT", result);
        assertEquals(1000.0, bankService.getBalance("ACC_SPRING_1"));

        System.out.println("Spring Boot Starter successfully integrated with Kurin Raft Core!");
    }
}