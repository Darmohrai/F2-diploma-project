package org.kurin.raft;

import org.kurin.network.NetworkStarter;
import org.kurin.network.client.KurinTransport;
import org.kurin.network.config.NetworkConfig;
import org.kurin.network.model.NodeAddress;
import org.kurin.network.serializer.KryoSerializer;
import org.kurin.raft.log.InMemoryRaftLog;
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

    public RaftNode(NodeAddress localAddress, Set<NodeAddress> peers, StateMachine stateMachine,
                    RaftConfig raftConfig, NetworkConfig networkConfig, KryoSerializer serializer,
                    List<RoleChangeListener> roleListeners, UnaryOperator<KurinTransport> transportDecorator) {
        AtomicReference<Thread> raftThread = new AtomicReference<>();
        CountDownLatch threadStarted = new CountDownLatch(1);
        this.raftExecutor = Executors.newSingleThreadExecutor(task -> {
            Thread thread = new Thread(task, "kurin-raft-" + localAddress.port());
            thread.setDaemon(true);
            raftThread.set(thread);
            return thread;
        });
        // Forces the executor to create its only thread now, so its identity is known for thread checks
        raftExecutor.execute(threadStarted::countDown);
        awaitUninterruptibly(threadStarted);

        this.raftState = new RaftState(raftConfig, localAddress, peers, new InMemoryRaftLog(), stateMachine,
                createMetadata(raftConfig, localAddress), serializer, roleListeners, raftExecutor, raftThread.get());

        RaftDispatcher dispatcher = new RaftDispatcher(raftState);
        this.networkStarter = new NetworkStarter(localAddress, dispatcher, networkConfig, serializer);
        this.raftState.setTransport(transportDecorator.apply(networkStarter.getClient()));
        this.raftState.setTimer(networkStarter.getTimer());
    }

    private static PersistentMetadata createMetadata(RaftConfig config, NodeAddress localAddress) {
        if (config.getStorage() == RaftConfig.StorageType.MEMORY) {
            return new InMemoryPersistentMetadata();
        }
        return new FilePersistentMetadata(resolveDataDir(config, localAddress));
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
    }

    public RaftState getRaftState() {
        return raftState;
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
