package org.kurin.test;

import org.awaitility.Awaitility;
import org.kurin.raft.KurinNode;
import org.kurin.raft.RaftConfig;
import org.kurin.raft.RaftStatus;

import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.BiConsumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * An in-process Kurin cluster on loopback for integration tests. Nodes talk over the real Netty transport,
 * wrapped so that tests can cut, delay or drop traffic between chosen nodes.
 *
 * <pre>{@code
 * try (KurinTestCluster cluster = KurinTestCluster.builder()
 *         .nodes(3)
 *         .services(() -> List.of(new CounterService()))
 *         .build()) {
 *     cluster.start();
 *     KurinTestCluster.Node leader = cluster.awaitLeader(Duration.ofSeconds(5));
 *     leader.kurin().submitCommand(new Increment()).get();
 * }
 * }</pre>
 */
public final class KurinTestCluster implements AutoCloseable {

    /**
     * One member of the cluster. The object survives {@link #restart}: only the node behind it is replaced.
     */
    public final class Node {
        private final int index;
        private final int port;
        private volatile KurinNode kurin;
        private volatile List<Object> services = List.of();
        private volatile boolean running;
        private final FaultInjectingStorage storageFaults = new FaultInjectingStorage();

        private Node(int index, int port) {
            this.index = index;
            this.port = port;
        }

        public int index() {
            return index;
        }

        public String nodeId() {
            return HOST + ":" + port;
        }

        /**
         * @return the current node instance; a new one after every restart
         */
        public KurinNode kurin() {
            return kurin;
        }

        public RaftStatus status() {
            return kurin.status();
        }

        public boolean isRunning() {
            return running;
        }

        /**
         * @return the service instance of the given type registered on this node
         */
        public <T> T service(Class<T> type) {
            return services.stream().filter(type::isInstance).map(type::cast).findFirst()
                    .orElseThrow(() -> new IllegalArgumentException("Node " + nodeId() + " has no service of type " + type.getName()));
        }

        @Override
        public String toString() {
            return "Node[" + nodeId() + "]";
        }
    }

    private static final String HOST = "127.0.0.1";

    private final List<Node> nodes = new ArrayList<>();
    private final Supplier<? extends Collection<?>> serviceFactory;
    private final RaftConfig.StorageType storage;
    private final Path dataDir;
    private final BiConsumer<Integer, KurinNode.Builder> customizer;
    private final NetworkFaults faults = new NetworkFaults();
    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor(task -> {
        Thread thread = new Thread(task, "kurin-test-faults");
        thread.setDaemon(true);
        return thread;
    });

    private KurinTestCluster(Builder builder) {
        this.serviceFactory = builder.serviceFactory;
        this.storage = builder.storage;
        this.dataDir = builder.dataDir;
        this.customizer = builder.customizer;
        for (int port : TestPorts.freePorts(builder.nodes)) {
            nodes.add(new Node(nodes.size(), port));
        }
    }

    public static Builder builder() {
        return new Builder();
    }

    // ---------------------------------------------------------------- lifecycle

    public KurinTestCluster start() {
        nodes.forEach(this::startNode);
        return this;
    }

    private void startNode(Node node) {
        KurinNode.Builder builder = KurinNode.builder()
                .localNode(HOST, node.port)
                .bindHost(HOST)
                .storage(storage)
                .addPeers(nodes.stream().map(Node::nodeId).toArray(String[]::new))
                .transportDecorator(transport -> new FaultInjectingTransport(node.nodeId(), transport, faults, scheduler))
                .storageDecorator(node.storageFaults::wrap);
        if (storage == RaftConfig.StorageType.FILE) {
            builder.dataDir(dataDir.resolve("node-" + node.index));
        }
        // Short timings keep tests fast while respecting electionTimeoutMin >= 5 * heartbeat
        builder.raftConfig().setHeartbeatIntervalMs(30);
        builder.raftConfig().setElectionTimeoutMinMs(300);
        builder.raftConfig().setElectionTimeoutMaxMs(600);
        builder.networkConfig().setConnectTimeoutMs(500);

        List<Object> services = new ArrayList<>(serviceFactory.get());
        services.forEach(builder::addService);
        customizer.accept(node.index, builder);

        KurinNode kurin = builder.build();
        kurin.start();
        node.services = List.copyOf(services);
        node.kurin = kurin;
        node.running = true;
    }

    /**
     * Stops the node abruptly from the point of view of its peers: its connections close and it stops answering.
     */
    public void kill(Node node) {
        if (node.running) {
            node.running = false;
            node.kurin.shutdown();
        }
    }

    /**
     * Starts a killed node again with fresh service instances. With MEMORY storage it comes back empty.
     */
    public void restart(Node node) {
        kill(node);
        startNode(node);
    }

    @Override
    public void close() {
        nodes.forEach(this::kill);
        scheduler.shutdownNow();
    }

    // ---------------------------------------------------------------- inspection

    public List<Node> nodes() {
        return List.copyOf(nodes);
    }

    public Node node(int index) {
        return nodes.get(index);
    }

    public List<Node> runningNodes() {
        return nodes.stream().filter(Node::isRunning).toList();
    }

    // Running nodes that take part in the cluster: a node with failed storage is up but applies nothing
    private List<Node> workingNodes() {
        return runningNodes().stream().filter(node -> !node.status().storageFailed()).toList();
    }

    /**
     * @return the running node with established leadership (valid lease, first entry of its term committed)
     * in the highest term, if there is one
     */
    public Optional<Node> leader() {
        return runningNodes().stream()
                .filter(node -> node.kurin().leadership().isPresent())
                .max((a, b) -> Long.compare(a.status().term(), b.status().term()));
    }

    public List<Node> followers() {
        Optional<Node> leader = leader();
        return runningNodes().stream().filter(node -> leader.isEmpty() || node != leader.get()).toList();
    }

    /**
     * Waits until a leader has established itself and every running node it can reach follows it.
     *
     * @throws org.awaitility.core.ConditionTimeoutException if no stable leader appears in time
     */
    public Node awaitLeader(Duration timeout) {
        return Awaitility.await("leader election")
                .atMost(timeout)
                .pollInterval(Duration.ofMillis(10))
                .until(this::establishedLeader, Optional::isPresent)
                .orElseThrow();
    }

    private Optional<Node> establishedLeader() {
        Optional<Node> leader = leader();
        if (leader.isEmpty()) {
            return Optional.empty();
        }
        String leaderId = leader.get().nodeId();
        long term = leader.get().status().term();
        // Nodes cut off from the leader by a partition cannot learn about it and are not waited for
        List<Node> reachable = runningNodes().stream()
                .filter(node -> !faults.isBlocked(leaderId, node.nodeId()) && !node.status().storageFailed())
                .toList();
        boolean acknowledged = reachable.stream()
                .allMatch(node -> leaderId.equals(node.status().leaderId()) && node.status().term() == term);
        // A leader that was just cut off still holds its lease for a moment; it reaches no majority and does not count
        return acknowledged && reachable.size() >= nodes.size() / 2 + 1 ? leader : Optional.empty();
    }

    /**
     * Waits until every running node whose storage works has applied at least the given log index.
     */
    public void awaitApplied(long logIndex, Duration timeout) {
        Awaitility.await("entry " + logIndex + " applied on all running nodes")
                .atMost(timeout)
                .pollInterval(Duration.ofMillis(10))
                .until(() -> workingNodes().stream().allMatch(node -> node.status().lastApplied() >= logIndex));
    }

    /**
     * Waits until every running node whose storage works has applied everything that is in the log of any of them:
     * all commands acknowledged so far are visible on all of them.
     */
    public void awaitInSync(Duration timeout) {
        Awaitility.await("all running nodes in sync")
                .atMost(timeout)
                .pollInterval(Duration.ofMillis(10))
                .until(() -> {
                    List<RaftStatus> statuses = workingNodes().stream().map(Node::status).toList();
                    long lastLogIndex = statuses.stream().mapToLong(RaftStatus::lastLogIndex).max().orElse(0);
                    return statuses.stream().allMatch(status -> status.lastApplied() >= lastLogIndex);
                });
    }

    // ---------------------------------------------------------------- faults

    /**
     * Splits the cluster: nodes of different groups cannot reach each other. Nodes not listed in any group
     * keep all their links.
     */
    @SafeVarargs
    public final void partition(Set<Node>... groups) {
        faults.partition(Arrays.stream(groups)
                .map(group -> group.stream().map(Node::nodeId).collect(Collectors.toSet()))
                .toList());
    }

    /**
     * Delays requests sent from one node to another. Responses travel back undelayed.
     */
    public void delay(Node from, Node to, Duration delay) {
        faults.delay(from.nodeId(), to.nodeId(), delay);
    }

    public void dropRate(Node from, Node to, double rate) {
        faults.dropRate(from.nodeId(), to.nodeId(), rate);
    }

    /**
     * Removes all partitions, delays and drop rates.
     */
    public void heal() {
        faults.heal();
    }

    /**
     * Makes every write of the node to its durable storage fail from now on, as a full or dying disk would.
     * The node notices at its next write and then leaves the cluster ({@code status().storageFailed()}).
     */
    public void breakStorage(Node node) {
        node.storageFaults.setBroken(true);
    }

    /**
     * Makes the storage of the node work again. The node finds out at its next retry and returns as a follower.
     */
    public void repairStorage(Node node) {
        node.storageFaults.setBroken(false);
    }

    /**
     * @return the switch of the node's storage faults, to break its log, snapshots or metadata separately
     */
    public FaultInjectingStorage storageFaults(Node node) {
        return node.storageFaults;
    }

    public static final class Builder {
        private int nodes = 3;
        private RaftConfig.StorageType storage = RaftConfig.StorageType.MEMORY;
        private Path dataDir;
        private Supplier<? extends Collection<?>> serviceFactory;
        private BiConsumer<Integer, KurinNode.Builder> customizer = (index, builder) -> {
        };

        public Builder nodes(int nodes) {
            if (nodes < 1) {
                throw new IllegalArgumentException("A cluster needs at least one node");
            }
            this.nodes = nodes;
            return this;
        }

        public Builder storage(RaftConfig.StorageType storage) {
            this.storage = storage;
            return this;
        }

        /**
         * @param dataDir parent directory of the per-node data directories; required for FILE storage.
         *                Pass a JUnit {@code @TempDir}.
         */
        public Builder dataDir(Path dataDir) {
            this.dataDir = dataDir;
            return this;
        }

        /**
         * @param serviceFactory creates the state machine services of one node; called once per node start,
         *                       because every node needs its own instances
         */
        public Builder services(Supplier<? extends Collection<?>> serviceFactory) {
            this.serviceFactory = serviceFactory;
            return this;
        }

        /**
         * Adjusts the builder of every node (node index, builder) after the cluster defaults were applied.
         */
        public Builder configure(BiConsumer<Integer, KurinNode.Builder> customizer) {
            this.customizer = customizer;
            return this;
        }

        public KurinTestCluster build() {
            if (serviceFactory == null) {
                throw new IllegalStateException("services(...) must be set");
            }
            if (storage == RaftConfig.StorageType.FILE && dataDir == null) {
                throw new IllegalStateException("dataDir(...) must be set for FILE storage");
            }
            return new KurinTestCluster(this);
        }
    }
}
