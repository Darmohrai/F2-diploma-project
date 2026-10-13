package org.kurin.raft;

import org.kurin.api.CommandOptions;
import org.kurin.api.KurinInstrumentation;
import org.kurin.api.KurinObserver;
import java.util.HashMap;
import java.util.Map;
import java.util.function.BiFunction;
import org.kurin.api.LeadershipToken;
import org.kurin.api.ReadConsistency;
import org.kurin.api.exception.KurinNotLeaderException;
import org.kurin.api.exception.KurinNotStartedException;
import org.kurin.api.exception.KurinTimeoutException;
import org.kurin.network.client.KurinTransport;
import org.kurin.network.config.NetworkConfig;
import org.kurin.network.dto.RpcResult;
import org.kurin.network.model.NodeAddress;
import org.kurin.network.serializer.KryoSerializer;
import org.kurin.raft.rpc.ClientCommandRequest;
import org.kurin.raft.rpc.ClientQueryRequest;
import org.kurin.raft.state.RaftState;
import org.kurin.raft.state.RoleChangeListener;
import org.kurin.raft.state.StateMachineListener;
import org.kurin.raft.tools.PojoStateMachine;
import org.kurin.serialization.KurinTypeRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.UnaryOperator;

/**
 * A member of a Kurin cluster embedded in the application.
 * <p>
 * A node is created first and started later. Between the two, services, listeners and types can still be
 * added; this lets a container create the node early (so that other components can depend on it) and
 * register the state machine services as it discovers them. {@link #start()} then validates the handlers,
 * freezes the type registry and joins the cluster.
 */
public class KurinNode {
    private static final Logger log = LoggerFactory.getLogger(KurinNode.class);

    private enum Lifecycle {NEW, STARTED, STOPPED}

    private final NodeAddress localAddress;
    private final Set<NodeAddress> peers;
    private final RaftConfig raftConfig;
    private final NetworkConfig networkConfig;
    private final KurinTypeRegistry typeRegistry;
    private final UnaryOperator<KurinTransport> transportDecorator;
    private final UnaryOperator<RaftStorage> storageDecorator;
    private final boolean allowServicesWithoutSnapshot;
    private final List<PojoStateMachine.Service> services;
    private final List<StateMachineListener> listeners;
    private final List<RoleChangeListener> roleListeners;
    private final List<KurinObserver> observers;
    private volatile KurinInstrumentation instrumentation;
    private final RaftStatus initialStatus;

    private volatile Lifecycle lifecycle = Lifecycle.NEW;
    private volatile RaftNode internalNode;
    private volatile RaftState raftState;
    private volatile KryoSerializer serializer;

    private KurinNode(Builder builder, NodeAddress localAddress, Set<NodeAddress> peers) {
        this.localAddress = localAddress;
        this.peers = Set.copyOf(peers);
        this.raftConfig = builder.raftConfig;
        this.networkConfig = builder.networkConfig;
        this.typeRegistry = builder.typeRegistry;
        this.transportDecorator = builder.transportDecorator;
        this.storageDecorator = builder.storageDecorator;
        this.allowServicesWithoutSnapshot = builder.allowServicesWithoutSnapshot;
        this.services = new ArrayList<>(builder.services);
        this.listeners = new ArrayList<>(builder.listeners);
        this.roleListeners = new ArrayList<>(builder.roleListeners);
        this.observers = new ArrayList<>(builder.observers);
        this.instrumentation = builder.instrumentation;
        this.initialStatus = new RaftStatus(RaftRole.FOLLOWER, 0, null, 0, 0, 0, 0, localAddress.asString());
    }

    // ---------------------------------------------------------------- configuration before start

    /**
     * Registers a state machine service: an object with {@code @KurinCommand}, {@code @KurinQuery},
     * {@code @KurinSnapshot} or {@code @KurinRestore} methods.
     *
     * @throws IllegalStateException if the node was already started
     */
    public synchronized KurinNode addService(Object service) {
        ensureNotStarted("add a service");
        services.add(PojoStateMachine.Service.of(service));
        return this;
    }

    /**
     * Registers a service whose annotated methods are declared by {@code type}, for instances that are proxies.
     */
    public synchronized KurinNode addService(Object service, Class<?> type) {
        ensureNotStarted("add a service");
        services.add(new PojoStateMachine.Service(service, type));
        return this;
    }

    public synchronized KurinNode addStateMachineListener(StateMachineListener listener) {
        ensureNotStarted("add a state machine listener");
        listeners.add(listener);
        return this;
    }

    public synchronized KurinNode addRoleChangeListener(RoleChangeListener listener) {
        ensureNotStarted("add a role change listener");
        roleListeners.add(listener);
        return this;
    }

    /**
     * Registers a type that travels inside commands, queries or results but cannot be discovered from
     * their declarations (for example an implementation behind an interface-typed field).
     */
    public synchronized KurinNode registerType(Class<?> type) {
        ensureNotStarted("register a type");
        typeRegistry.register(type);
        return this;
    }

    /**
     * Registers a receiver of the measurements of this node.
     */
    public synchronized KurinNode addObserver(KurinObserver observer) {
        ensureNotStarted("add an observer");
        observers.add(observer);
        return this;
    }

    /**
     * Connects the node to a tracing system.
     */
    public synchronized KurinNode instrumentation(KurinInstrumentation instrumentation) {
        ensureNotStarted("set the instrumentation");
        this.instrumentation = instrumentation;
        return this;
    }

    public KurinTypeRegistry typeRegistry() {
        return typeRegistry;
    }

    private void ensureNotStarted(String action) {
        if (lifecycle != Lifecycle.NEW) {
            throw new IllegalStateException("Cannot " + action + ": node " + nodeId() + " is already started. "
                    + "The set of services and types is fixed when the node starts.");
        }
    }

    // ---------------------------------------------------------------- lifecycle

    /**
     * Validates the registered services, freezes the type registry, restores the durable state and joins the cluster.
     *
     * @throws IllegalStateException if the node was already started, a handler method is invalid, two types
     *                               claim the same wire id, the stored state is unusable or the port cannot be bound
     */
    public synchronized void start() {
        if (lifecycle != Lifecycle.NEW) {
            throw new IllegalStateException("Node " + nodeId() + " cannot be started twice");
        }
        PojoStateMachine stateMachine = new PojoStateMachine(services, allowServicesWithoutSnapshot);
        stateMachine.messageHandlerMethods().forEach(typeRegistry::registerHandler);
        typeRegistry.freeze();
        KryoSerializer nodeSerializer = new KryoSerializer(typeRegistry);
        stateMachine.bindSerializer(nodeSerializer);
        listeners.forEach(stateMachine::addListener);

        RaftNode node = new RaftNode(localAddress, peers, stateMachine, raftConfig, networkConfig, nodeSerializer,
                roleListeners, transportDecorator, storageDecorator, observers, instrumentation);
        try {
            node.start();
        } catch (RuntimeException e) {
            node.shutdown();
            throw e;
        }
        this.serializer = nodeSerializer;
        this.raftState = node.getRaftState();
        this.internalNode = node;
        this.lifecycle = Lifecycle.STARTED;
        log.info("Kurin node {} started with peers {} and {} service(s)", nodeId(), peers, services.size());
    }

    public synchronized void shutdown() {
        if (lifecycle == Lifecycle.STOPPED) {
            return;
        }
        boolean wasStarted = lifecycle == Lifecycle.STARTED;
        lifecycle = Lifecycle.STOPPED;
        if (wasStarted) {
            internalNode.shutdown();
            log.info("Kurin node {} stopped", nodeId());
        }
    }

    public boolean isStarted() {
        return lifecycle == Lifecycle.STARTED;
    }

    // ---------------------------------------------------------------- commands

    /**
     * Replicates the command and applies it on every node. Waits for the node default timeout.
     *
     * @return the value returned by the {@code @KurinCommand} handler; the future fails with a
     * {@link org.kurin.api.exception.KurinException}
     */
    public <T> CompletableFuture<T> submitCommand(Object command) {
        return submitCommand(command, CommandOptions.defaults());
    }

    public <T> CompletableFuture<T> submitCommand(Object command, Duration timeout) {
        return submitCommand(command, CommandOptions.withTimeout(timeout));
    }

    /**
     * @param options request id for idempotent retries and timeout. After a {@link KurinTimeoutException} the
     *                command may or may not have been applied; resubmitting with the same request id is safe.
     */
    public <T> CompletableFuture<T> submitCommand(Object command, CommandOptions options) {
        if (command == null) {
            throw new IllegalArgumentException("command must not be null");
        }
        String requestId = options.requestId() != null ? options.requestId() : UUID.randomUUID().toString();
        Duration timeout = options.timeout() != null ? options.timeout() : Duration.ofMillis(raftConfig.getCommandTimeoutMs());
        String type = command.getClass().getSimpleName();
        return submit(timeout, "kurin.command", type, "Command " + type + " (request " + requestId + ")",
                (state, headers) -> state.getCurrentRole().handleClientCommand(state,
                        new ClientCommandRequest(requestId, command, headers), true));
    }

    /**
     * Runs a {@code @KurinQuery} handler with {@link ReadConsistency#LINEARIZABLE} consistency.
     */
    public <T> CompletableFuture<T> submitQuery(Object query) {
        return submitQuery(query, ReadConsistency.LINEARIZABLE);
    }

    /**
     * @param consistency how fresh the observed state must be; see {@link ReadConsistency} for the trade-offs
     */
    public <T> CompletableFuture<T> submitQuery(Object query, ReadConsistency consistency) {
        if (query == null || consistency == null) {
            throw new IllegalArgumentException("query and consistency must not be null");
        }
        String requestId = UUID.randomUUID().toString();
        String type = query.getClass().getSimpleName();
        return submit(Duration.ofMillis(raftConfig.getCommandTimeoutMs()), "kurin.query", type, "Query " + type,
                (state, headers) -> state.getCurrentRole().handleClientQuery(state,
                        new ClientQueryRequest(requestId, query, consistency, headers), true));
    }

    @SuppressWarnings("unchecked")
    private <T> CompletableFuture<T> submit(Duration timeout, String spanName, String type, String description,
                                            BiFunction<RaftState, HashMap<String, String>, CompletableFuture<Object>> request) {
        CompletableFuture<Object> result = new CompletableFuture<>();
        RaftNode node = internalNode;
        if (lifecycle != Lifecycle.STARTED || node == null) {
            result.completeExceptionally(new KurinNotStartedException("Node " + nodeId()
                    + (lifecycle == Lifecycle.STOPPED ? " is shut down" : " is not started yet")));
            return (CompletableFuture<T>) result;
        }

        var timeoutTask = node.getTimer().newTimeout(expired -> result.completeExceptionally(new KurinTimeoutException(
                        description + " got no result within " + timeout.toMillis() + " ms. It may still be applied.")),
                timeout.toMillis(), TimeUnit.MILLISECONDS);
        result.whenComplete((value, error) -> timeoutTask.cancel());

        // Started on the calling thread, so that it continues the trace of the caller
        RaftState submittedTo = node.getRaftState();
        KurinInstrumentation.Span span = submittedTo.startSpan(spanName, Map.of("type", type), null);
        Map<String, String> spanHeaders = span.headers();
        HashMap<String, String> headers = spanHeaders == null || spanHeaders.isEmpty() ? null : new HashMap<>(spanHeaders);
        result.whenComplete((value, error) -> submittedTo.endSpan(span, error));
        Function<RaftState, CompletableFuture<Object>> operation = state -> request.apply(state, headers);

        attempt(submittedTo, operation, result, true);
        return (CompletableFuture<T>) result;
    }

    private void attempt(RaftState state, Function<RaftState, CompletableFuture<Object>> operation,
                         CompletableFuture<Object> result, boolean mayRetry) {
        try {
            state.execute(() -> {
                CompletableFuture<Object> attempt;
                try {
                    attempt = operation.apply(state);
                } catch (RuntimeException e) {
                    attempt = CompletableFuture.failedFuture(e);
                }
                attempt.whenComplete((value, error) -> {
                    Throwable cause = error != null ? RpcResult.unwrap(error) : null;
                    if (cause instanceof KurinNotLeaderException && mayRetry && !result.isDone()) {
                        // This node lost leadership while the request was in flight: route it once more
                        attempt(state, operation, result, false);
                    } else if (cause != null) {
                        result.completeExceptionally(cause);
                    } else {
                        result.complete(value);
                    }
                });
            });
        } catch (KurinNotStartedException e) {
            result.completeExceptionally(e);
        }
    }

    // ---------------------------------------------------------------- status

    /**
     * @return a consistent snapshot of the node state; never blocks. Before the node is started it reports
     * a follower in term 0 without a leader.
     */
    public RaftStatus status() {
        RaftState state = raftState;
        return state != null ? state.status() : initialStatus;
    }

    public boolean isLeader() {
        return status().isLeader();
    }

    /**
     * @return a token if this node is the leader right now with certainty: it holds a valid lease and the first
     * entry of its term is committed. Empty on followers, on a leader that lost contact with the majority and
     * on a freshly elected leader that has not established itself yet.
     */
    public Optional<LeadershipToken> leadership() {
        RaftState state = raftState;
        if (state == null || lifecycle != Lifecycle.STARTED) {
            return Optional.empty();
        }
        LeadershipToken token = state.leadershipToken();
        RaftStatus current = state.status();
        if (token != null && current.hasValidLease() && current.term() == token.term()) {
            return Optional.of(token);
        }
        return Optional.empty();
    }

    /**
     * Starts a span in the tracing system of the node, for modules built on top of it.
     *
     * @return a span that must be ended; a no-op one if no tracing is connected or the node is not started
     */
    public KurinInstrumentation.Span startSpan(String name, Map<String, String> tags) {
        RaftState state = raftState;
        return state != null ? state.startSpan(name, tags, null) : KurinInstrumentation.Span.NOOP;
    }

    /**
     * @return number of entries currently held in the Raft log (those not yet replaced by a snapshot)
     */
    public long logEntries() {
        RaftState state = raftState;
        return state != null ? state.logEntries() : 0;
    }

    /**
     * @return for how long this node has known no leader; zero while it knows one or is not started
     */
    public Duration leaderUnknownFor() {
        RaftState state = raftState;
        return state != null && lifecycle == Lifecycle.STARTED ? Duration.ofNanos(state.leaderUnknownForNanos()) : Duration.ZERO;
    }

    /**
     * @return how long ago this node last heard from each peer it is in contact with in its current role:
     * on a leader its followers, on a follower its leader
     */
    public Map<String, Duration> peerContactAges() {
        RaftState state = raftState;
        Map<String, Duration> ages = new java.util.TreeMap<>();
        if (state != null) {
            long now = System.nanoTime();
            state.peerContactNanos().forEach((peer, contact) -> ages.put(peer, Duration.ofNanos(Math.max(0, now - contact))));
        }
        return ages;
    }

    public String nodeId() {
        return localAddress.asString();
    }

    public NodeAddress getLocalAddress() {
        return localAddress;
    }

    public Set<NodeAddress> peers() {
        return peers;
    }

    public RaftConfig config() {
        return raftConfig;
    }

    public NetworkConfig networkConfig() {
        return networkConfig;
    }

    /**
     * @return the serializer of this node, restricted to the registered types
     * @throws KurinNotStartedException before the node is started, because the set of types is not final yet
     */
    public KryoSerializer serializer() {
        KryoSerializer current = serializer;
        if (current == null) {
            throw new KurinNotStartedException("The serializer of node " + nodeId() + " exists only after the node is started");
        }
        return current;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private String advertisedHost;
        private int port;
        private final Set<NodeAddress> peers = new LinkedHashSet<>();
        private final RaftConfig raftConfig = new RaftConfig();
        private final NetworkConfig networkConfig = new NetworkConfig();
        private final KurinTypeRegistry typeRegistry = new KurinTypeRegistry();
        private final List<PojoStateMachine.Service> services = new ArrayList<>();
        private final List<StateMachineListener> listeners = new ArrayList<>();
        private final List<RoleChangeListener> roleListeners = new ArrayList<>();
        private UnaryOperator<KurinTransport> transportDecorator = UnaryOperator.identity();
        private UnaryOperator<RaftStorage> storageDecorator = UnaryOperator.identity();
        private boolean allowServicesWithoutSnapshot;
        private final List<KurinObserver> observers = new ArrayList<>();
        private KurinInstrumentation instrumentation = KurinInstrumentation.NOOP;

        public Builder addObserver(KurinObserver observer) {
            this.observers.add(observer);
            return this;
        }

        public Builder instrumentation(KurinInstrumentation instrumentation) {
            this.instrumentation = instrumentation;
            return this;
        }

        /**
         * @param advertisedHost the host other nodes use to reach this node; together with the port it forms the node id
         */
        public Builder localNode(String advertisedHost, int port) {
            this.advertisedHost = advertisedHost;
            this.port = port;
            return this;
        }

        /**
         * @param bindHost the local interface to listen on, {@code 0.0.0.0} by default
         */
        public Builder bindHost(String bindHost) {
            networkConfig.setBindHost(bindHost);
            return this;
        }

        public Builder clusterName(String clusterName) {
            networkConfig.setClusterName(clusterName);
            return this;
        }

        /**
         * Adds cluster members as {@code host:port}. The list may include this node: it is filtered out,
         * so every node can be given the same configuration.
         */
        public Builder addPeers(String... peerAddresses) {
            for (String peer : peerAddresses) {
                if (peer != null && !peer.isBlank()) {
                    peers.add(NodeAddress.parse(peer));
                }
            }
            return this;
        }

        public Builder addService(Object service) {
            this.services.add(PojoStateMachine.Service.of(service));
            return this;
        }

        /**
         * Adds a service whose annotated methods are declared by {@code type}, for instances that are proxies.
         */
        public Builder addService(Object service, Class<?> type) {
            this.services.add(new PojoStateMachine.Service(service, type));
            return this;
        }

        /**
         * Registers a type that travels inside commands, queries or results but cannot be discovered from
         * their declarations (for example an implementation behind an interface-typed field).
         */
        public Builder registerType(Class<?> type) {
            typeRegistry.register(type);
            return this;
        }

        public Builder addStateMachineListener(StateMachineListener listener) {
            this.listeners.add(listener);
            return this;
        }

        public Builder addRoleChangeListener(RoleChangeListener listener) {
            this.roleListeners.add(listener);
            return this;
        }

        public Builder storage(RaftConfig.StorageType storage) {
            raftConfig.setStorage(storage);
            return this;
        }

        public Builder dataDir(Path dataDir) {
            raftConfig.setDataDir(dataDir);
            return this;
        }

        /**
         * Wraps the transport used to reach peers. Meant for fault injection in tests.
         */
        public Builder transportDecorator(UnaryOperator<KurinTransport> transportDecorator) {
            this.transportDecorator = transportDecorator;
            return this;
        }

        /**
         * Wraps the durable storage of the node (log, snapshots, term and vote), or replaces it with another
         * implementation. Meant for fault injection in tests and for custom storage back ends.
         */
        public Builder storageDecorator(UnaryOperator<RaftStorage> storageDecorator) {
            this.storageDecorator = storageDecorator;
            return this;
        }

        /**
         * Lets the node start with services that handle commands, are not declared stateless and have no
         * snapshot methods. Their state is lost on every node that restarts or catches up after the log was
         * compacted; a warning is logged for each. By default such a service fails the start.
         */
        public Builder allowServicesWithoutSnapshot(boolean allowServicesWithoutSnapshot) {
            this.allowServicesWithoutSnapshot = allowServicesWithoutSnapshot;
            return this;
        }

        public RaftConfig raftConfig() {
            return raftConfig;
        }

        public NetworkConfig networkConfig() {
            return networkConfig;
        }

        public KurinTypeRegistry typeRegistry() {
            return typeRegistry;
        }

        /**
         * Validates the configuration and creates the node without starting it. Services, listeners and types
         * can still be added to the node until {@link KurinNode#start()}.
         *
         * @throws IllegalArgumentException on invalid configuration
         */
        public KurinNode build() {
            if (port <= 0 || port > 65535) {
                throw new IllegalArgumentException("A node port between 1 and 65535 must be configured, got " + port);
            }
            NodeAddress self = resolveSelf();
            Set<NodeAddress> otherPeers = new LinkedHashSet<>(peers);
            otherPeers.remove(self);
            warnIfLoopbackAdvertised(self, otherPeers);

            if (raftConfig.getMaxAppendBytes() <= 0) {
                raftConfig.setMaxAppendBytes(networkConfig.getMaxPayloadSize() / 2);
            }
            if (raftConfig.getMaxAppendBytes() > networkConfig.getMaxPayloadSize()) {
                throw new IllegalArgumentException("maxAppendBytes (" + raftConfig.getMaxAppendBytes()
                        + ") must not exceed maxPayloadSize (" + networkConfig.getMaxPayloadSize() + ")");
            }
            raftConfig.validate();

            return new KurinNode(this, self, otherPeers);
        }

        private NodeAddress resolveSelf() {
            if (advertisedHost != null && !advertisedHost.isBlank()) {
                return new NodeAddress(advertisedHost.trim(), port);
            }
            if (peers.stream().allMatch(peer -> isLoopback(peer.host()))) {
                // Single node or an all-local cluster: loopback is the only sensible identity
                return new NodeAddress("127.0.0.1", port);
            }
            throw new IllegalArgumentException("advertisedHost must be set when peers are configured: "
                    + "it is the address other nodes use to reach this node and part of its identity");
        }

        private static void warnIfLoopbackAdvertised(NodeAddress self, Set<NodeAddress> otherPeers) {
            if (isLoopback(self.host()) && otherPeers.stream().anyMatch(peer -> !isLoopback(peer.host()))) {
                log.warn("Node advertises the loopback address {} but has non-local peers {}. "
                        + "Other machines will not be able to connect back to it.", self.asString(), otherPeers);
            }
        }

        private static boolean isLoopback(String host) {
            if ("localhost".equalsIgnoreCase(host)) {
                return true;
            }
            // Only literal addresses are checked: resolving names here could block on DNS
            if (!host.matches("[0-9.]+") && !host.contains(":")) {
                return false;
            }
            try {
                return InetAddress.getByName(host).isLoopbackAddress();
            } catch (UnknownHostException e) {
                return false;
            }
        }
    }
}
