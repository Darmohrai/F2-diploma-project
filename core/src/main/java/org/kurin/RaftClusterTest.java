package org.kurin;

import org.kurin.api.KurinCommand;
import org.kurin.api.KurinRestore;
import org.kurin.api.KurinSnapshot;
import org.kurin.raft.KurinNode;

import java.io.*;
import java.util.HashMap;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;

public class RaftClusterTest {

    // --- 1. DTO (Команди) ---
    public record AddFundsCommand(String userId, double amount) {}
    public record AddStockCommand(String itemId, int quantity) {}

    // --- 2. БІЛІНГ (POJO) ---
    public static class BillingService {
        private final HashMap<String, Double> balances = new HashMap<>();

        @KurinCommand
        public String handleFunds(AddFundsCommand cmd) {
            double newBalance = balances.getOrDefault(cmd.userId(), 0.0) + cmd.amount();
            balances.put(cmd.userId(), newBalance);
            System.out.println(">>> [BILLING] Account " + cmd.userId() + " updated. Balance: " + newBalance);
            return "FUNDS_ADDED";
        }

        @KurinSnapshot
        public byte[] backup() throws IOException {
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            new ObjectOutputStream(bos).writeObject(balances);
            return bos.toByteArray();
        }

        @SuppressWarnings("unchecked")
        @KurinRestore
        public void recover(byte[] data) throws Exception {
            HashMap<String, Double> state = (HashMap<String, Double>) new ObjectInputStream(new ByteArrayInputStream(data)).readObject();
            balances.clear();
            balances.putAll(state);
        }
    }

    // --- 3. СКЛАД (POJO) ---
    public static class InventoryService {
        private final HashMap<String, Integer> stock = new HashMap<>();

        @KurinCommand
        public String handleStock(AddStockCommand cmd) {
            int newStock = stock.getOrDefault(cmd.itemId(), 0) + cmd.quantity();
            stock.put(cmd.itemId(), newStock);
            System.out.println(">>> [INVENTORY] Item " + cmd.itemId() + " added. Stock: " + newStock);
            return "STOCK_ADDED";
        }

        @KurinSnapshot
        public byte[] backup() throws IOException {
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            new ObjectOutputStream(bos).writeObject(stock);
            return bos.toByteArray();
        }

        @SuppressWarnings("unchecked")
        @KurinRestore
        public void recover(byte[] data) throws Exception {
            HashMap<String, Integer> state = (HashMap<String, Integer>) new ObjectInputStream(new ByteArrayInputStream(data)).readObject();
            stock.clear();
            stock.putAll(state);
        }
    }

    // --- 4. ЗАПУСК ---
    public static void main(String[] args) throws Exception {
        System.out.println("Initializing Cluster Nodes...");

        BillingService billingService = new BillingService();
        InventoryService inventoryService = new InventoryService();

        KurinNode node1 = createNode("9011", "127.0.0.1:9012,127.0.0.1:9013", billingService, inventoryService);
        KurinNode node2 = createNode("9012", "127.0.0.1:9011,127.0.0.1:9013", billingService, inventoryService);
        KurinNode node3 = createNode("9013", "127.0.0.1:9011,127.0.0.1:9012", billingService, inventoryService);

        node1.start(); node2.start(); node3.start();
        Thread.sleep(2000); // Очікування виборів лідера

        System.out.println("\n--- TEST 1: MULTIPLE SERVICES ROUTING ---");
        // Сабмітимо на node1 (навіть якщо вона фоловер, спрацює Transparent Forwarding)
        CompletableFuture<Object> f1 = node1.submitCommand(new AddFundsCommand("USER_VADYM", 1500.0));
        CompletableFuture<Object> f2 = node1.submitCommand(new AddStockCommand("MACBOOK_PRO", 5));

        System.out.println("Result 1: " + f1.get());
        System.out.println("Result 2: " + f2.get());
        Thread.sleep(1000);

        System.out.println("\n--- TEST 2: AGGREGATED SNAPSHOT ---");
        // Це викличе takeSnapshot() в ОБОХ сервісах і склеїть їх в єдиний байтовий масив
        node1.triggerSnapshot();
        Thread.sleep(1000);

        System.out.println("\n--- SHUTTING DOWN ---");
        node1.shutdown(); node2.shutdown(); node3.shutdown();
    }

    private static KurinNode createNode(String port, String peers, Object... services) {
        Properties props = new Properties();
        props.setProperty("kurin.node.host", "127.0.0.1");
        props.setProperty("kurin.node.port", port);
        props.setProperty("kurin.node.peers", peers);

        KurinNode.Builder builder = KurinNode.builder()
                .fromProperties(props)
                .registerCommand(AddFundsCommand.class)
                .registerCommand(AddStockCommand.class);

        for (Object s : services) builder.addService(s);
        return builder.build();
    }
}