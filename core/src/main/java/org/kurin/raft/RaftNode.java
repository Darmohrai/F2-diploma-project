package org.kurin.raft;

import org.kurin.network.NetworkStarter;
import org.kurin.network.client.KurinTransport;
import org.kurin.network.config.NetworkConfig;
import org.kurin.network.model.NodeAddress;
import org.kurin.network.serializer.KryoSerializer;
import org.kurin.raft.log.FileRaftLog;
import org.kurin.raft.log.InMemoryRaftLog;
import org.kurin.raft.log.RaftLog;
import org.kurin.raft.snapshot.FileSnapshotStore;
import org.kurin.raft.snapshot.InMemorySnapshotStore;
import org.kurin.raft.snapshot.SnapshotStore;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import org.kurin.raft.state.FilePersistentMetadata;
import org.kurin.raft.state.InMemoryPersistentMetadata;
import org.kurin.raft.state.NodeRole;
import org.kurin.raft.state.PersistentMetadata;
import org.kurin.raft.state.RaftState;
import org.kurin.raft.state.RoleChangeListener;
import org.kurin.raft.state.StateMachine;
import org.kurin.raft.state.roles.FollowerRole;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.UnaryOperator;

/**
 * Wires the Raft state, its single thread and the network of one node.
 */
public class RaftNode {
    private static final Logger log = LoggerFactory.getLogger(RaftNode.class);

    private final NetworkStarter networkStarter;
    private final RaftState raftState;
    private final ExecutorService raftExecutor;
    private final RaftLog raftLog;

    public RaftNode(NodeAddress localAddress, Set<NodeAddress> peers, StateMachine stateMachine,
                    RaftConfig raftConfig, NetworkConfig networkConfig, KryoSerializer serializer,
                    List<RoleChangeListener> roleListeners, UnaryOperator<KurinTransport> transportDecorator,
                    UnaryOperator<RaftStorage> storageDecorator,
                    List<org.kurin.api.KurinObserver> observers, org.kurin.api.KurinInstrumentation instrumentation) {
        AtomicReference<Thread> raftThread = new AtomicReference<>();
        CountDownLatch threadStarted = new CountDownLatch(1);
        LinkedBlockingQueue<Runnable> raftQueue = new LinkedBlockingQueue<>();
        this.raftExecutor = new ThreadPoolExecutor(1, 1, 0, TimeUnit.MILLISECONDS, raftQueue, task -> {
            Thread thread = new Thread(task, "kurin-raft-" + localAddress.port());
            thread.setDaemon(true);
            raftThread.set(thread);
            return thread;
        });
        // Forces the executor to create its only thread now, so its identity is known for thread checks
        raftExecutor.execute(threadStarted::countDown);
        awaitUninterruptibly(threadStarted);

        RaftStorage builtIn;
        if (raftConfig.getStorage() == RaftConfig.StorageType.MEMORY) {
            builtIn = new RaftStorage(new InMemoryRaftLog(), new InMemorySnapshotStore(), new InMemoryPersistentMetadata());
        } else {
            Path dataDir = resolveDataDir(raftConfig, localAddress);
            builtIn = new RaftStorage(new FileRaftLog(dataDir.resolve("log"), serializer),
                    new FileSnapshotStore(dataDir, serializer), new FilePersistentMetadata(dataDir));
            log.info("Node {} keeps its state in {}", localAddress.asString(), dataDir.toAbsolutePath());
        }
        RaftStorage storage = storageDecorator.apply(builtIn);
        PersistentMetadata metadata = storage.metadata();
        SnapshotStore snapshotStore = storage.snapshots();
        this.raftLog = storage.log();

        this.raftState = new RaftState(raftConfig, localAddress, peers, raftLog, stateMachine, metadata, serializer,
                roleListeners, raftExecutor, raftThread.get(), snapshotStore, () -> !raftQueue.isEmpty());

        RaftDispatcher dispatcher = new RaftDispatcher(raftState);
        this.networkStarter = new NetworkStarter(localAddress, dispatcher, networkConfig, serializer);
        this.raftState.setObservability(observers, instrumentation);
        KurinTransport transport = transportDecorator.apply(networkStarter.getClient());
        // Every request to a peer is timed, whatever sends it
        this.raftState.setTransport((destination, payload, timeout) -> {
            long startedNanos = System.nanoTime();
            String type = payload.getClass().getSimpleName();
            return transport.sendRequest(destination, payload, timeout).whenComplete((response, error) -> {
                long latencyNanos = System.nanoTime() - startedNanos;
                raftState.observe(observer -> observer.onRpc(type, destination.asString(), latencyNanos, error == null));
            });
        });
        this.raftState.setTimer(networkStarter.getTimer());
    }

    public static Path resolveDataDir(RaftConfig config, NodeAddress localAddress) {
        if (config.getDataDir() != null) {
            return config.getDataDir();
        }
        String directoryName = (localAddress.host() + "-" + localAddress.port()).replaceAll("[^A-Za-z0-9._-]", "_");
        return Path.of("kurin-data", directoryName);
    }

    /**
     * @throws IllegalStateException if the listening port cannot be bound
     */
    public void start() {
        // Durable state is restored before the node can be reached: a failure here must fail the start
        CompletableFuture<Void> restored = new CompletableFuture<>();
        raftState.execute(() -> {
            try {
                raftState.restoreFromStorage();
                restored.complete(null);
            } catch (Throwable t) {
                restored.completeExceptionally(t);
            }
        });
        try {
            restored.join();
        } catch (CompletionException e) {
            throw new IllegalStateException("Node " + raftState.getLocalAddress().asString()
                    + " cannot restore its state: " + e.getCause().getMessage(), e.getCause());
        }

        networkStarter.start();
        raftState.execute(() -> raftState.transitionTo(new FollowerRole()));
    }

    public void shutdown() {
        CountDownLatch roleExited = new CountDownLatch(1);
        try {
            raftState.execute(() -> {
                try {
                    NodeRole role = raftState.getCurrentRole();
                    if (role != null) {
                        role.onExit(raftState);
                    }
                    raftState.failPendingClientRequests(new org.kurin.api.exception.KurinNotStartedException(
                            "Node " + raftState.getLocalAddress().asString() + " is shutting down"));
                } finally {
                    roleExited.countDown();
                }
            });
            if (!roleExited.await(2, TimeUnit.SECONDS)) {
                log.warn("Raft thread of {} did not finish its role in time", raftState.getLocalAddress().asString());
            }
        } catch (org.kurin.api.exception.KurinNotStartedException e) {
            log.debug("Node {} is already shut down", raftState.getLocalAddress().asString());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        raftExecutor.shutdown();
        networkStarter.shutdown();
        try {
            if (!raftExecutor.awaitTermination(2, TimeUnit.SECONDS)) {
                raftExecutor.shutdownNow();
            }
        } catch (InterruptedException e) {
            raftExecutor.shutdownNow();
            Thread.currentThread().interrupt();
        }
        try {
            raftLog.close();
        } catch (RuntimeException e) {
            // Everything acknowledged was flushed when it was acknowledged; a log that cannot be closed loses nothing
            log.warn("The Raft log of {} could not be closed cleanly: {}", raftState.getLocalAddress().asString(), e.toString());
        }
    }

    public RaftState getRaftState() {
        return raftState;
    }

    /**
     * @return the state of this node's own connection to the peer, whatever decorates the transport
     */
    public org.kurin.network.client.ConnectionManager.Reachability reachability(NodeAddress peer) {
        return networkStarter.getClient().reachability(peer);
    }

    public io.netty.util.Timer getTimer() {
        return networkStarter.getTimer();
    }

    private static void awaitUninterruptibly(CountDownLatch latch) {
        boolean interrupted = false;
        while (true) {
            try {
                latch.await();
                break;
            } catch (InterruptedException e) {
                interrupted = true;
            }
        }
        if (interrupted) {
            Thread.currentThread().interrupt();
        }
    }
}
