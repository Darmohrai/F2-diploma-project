package org.kurin.kurinlocks;

import org.kurin.kurinlocks.api.KurinLockManager;
import org.kurin.kurinlocks.command.AcquireLockCommand;
import org.kurin.kurinlocks.command.ReleaseLockCommand;
import org.kurin.kurinlocks.state.DistributedLockService;
import org.kurin.raft.KurinNode;

public class KurinLocksPlayground {

    public static void main(String[] args) throws Exception {
        System.out.println("=== Ініціалізація кластера з 3-х нод ===");

        DistributedLockService lockService = new DistributedLockService();

        KurinNode node1 = createNode(9011, "127.0.0.1:9012", "127.0.0.1:9013", lockService);
        KurinNode node2 = createNode(9012, "127.0.0.1:9011", "127.0.0.1:9013", lockService);
        KurinNode node3 = createNode(9013, "127.0.0.1:9011", "127.0.0.1:9012", lockService);

        node1.start();
        node2.start();
        node3.start();

        System.out.println("\n[Кластер] Чекаємо 3 секунди на вибори лідера (Leader Election)...");
        Thread.sleep(3000);

        System.out.println("\n=== Тестування розподілених блокувань (Distributed Locks) ===");

        // Створюємо клієнтські менеджери для Ноди 1 та Ноди 2
        KurinLockManager lockManagerNode1 = new KurinLockManager(node1);
        KurinLockManager lockManagerNode2 = new KurinLockManager(node2);

        String resourceKey = "drone-123-route";

        // 1. Нода 1 намагається захопити лок
        System.out.println("\n---> Нода 1 пробує захопити лок '" + resourceKey + "'...");
        boolean acquiredByNode1 = lockManagerNode1.tryLock(resourceKey, 5000);
        System.out.println("Результат Ноди 1: " + (acquiredByNode1 ? "УСПІХ (Лок захоплено)" : "ВІДМОВА"));

        // 2. Нода 2 намагається захопити ТОЙ САМИЙ лок (має отримати відмову)
        System.out.println("\n---> Нода 2 пробує захопити лок '" + resourceKey + "' (має бути відмова)...");
        boolean acquiredByNode2 = lockManagerNode2.tryLock(resourceKey, 5000);
        System.out.println("Результат Ноди 2: " + (acquiredByNode2 ? "УСПІХ" : "ВІДМОВА (Лок зайнятий Нодою 1)"));

        // 3. Нода 1 звільняє лок
        System.out.println("\n---> Нода 1 звільняє лок '" + resourceKey + "'...");
        lockManagerNode1.unlock(resourceKey);
        Thread.sleep(500); // Чекаємо реплікації зняття лока

        // 4. Нода 2 знову пробує захопити лок (тепер має вийти)
        System.out.println("\n---> Нода 2 знову пробує захопити лок '" + resourceKey + "'...");
        boolean acquiredByNode2Again = lockManagerNode2.tryLock(resourceKey, 5000);
        System.out.println("Результат Ноди 2: " + (acquiredByNode2Again ? "УСПІХ (Лок захоплено)" : "ВІДМОВА"));

        System.out.println("\n=== Тестування успішне! Зупиняємо кластер... ===");
        node1.shutdown();
        node2.shutdown();
        node3.shutdown();
        System.exit(0);
    }

    private static KurinNode createNode(int port, String peer1, String peer2, DistributedLockService lockService) {
        return KurinNode.builder()
                .localNode("127.0.0.1", port)
                .addPeers(peer1, peer2)
                .addService(lockService) // Реєструємо сервіс у StateMachine
                .registerCommand(AcquireLockCommand.class) // Реєструємо DTO для Kryo
                .registerCommand(ReleaseLockCommand.class)
                .build();
    }
}