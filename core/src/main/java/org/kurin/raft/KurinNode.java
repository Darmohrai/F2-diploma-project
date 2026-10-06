package org.kurin.raft;

import org.kurin.network.config.NetworkConfig;
import org.kurin.network.model.NodeAddress;
import org.kurin.network.serializer.KryoSerializer;
import org.kurin.raft.rpc.ClientCommandRequest;
import org.kurin.raft.state.RaftState;
import org.kurin.raft.state.StateMachine;
import org.kurin.raft.state.StateMachineListener;
import org.kurin.raft.tools.PojoStateMachine;

import java.util.*;
import java.util.concurrent.CompletableFuture;

public class KurinNode {
    private final RaftNode internalNode;

    public KurinNode(RaftNode internalNode) {
        this.internalNode = internalNode;
    }

    public void start() {
        internalNode.start();
    }

    public void shutdown() {
        internalNode.shutdown();
    }

    public <T> CompletableFuture<T> submitCommand(Object command) {
        String uniqueId = UUID.randomUUID().toString();
        ClientCommandRequest request = new ClientCommandRequest(uniqueId, command);
        CompletableFuture<Object> resultFuture = new CompletableFuture<>();

        internalNode.getRaftState().getMailbox().submit(() -> {
            try {
                internalNode.getRaftState().getCurrentRole().handleClientCommand(internalNode.getRaftState(), request)
                        .whenComplete((result, throwable) -> {
                            if (throwable != null) {
                                resultFuture.completeExceptionally(throwable);
                            } else {
                                resultFuture.complete(result);
                            }
                        });
            } catch (Exception e) {
                resultFuture.completeExceptionally(e);
            }
        });

        return resultFuture.thenApply(res -> (T) res);
    }

    public RaftState getRaftState() {
        return internalNode.getRaftState();
    }

    public NodeAddress getLocalAddress() {
        return internalNode.getRaftState().getLocalAddress();
    }

    public boolean isLeader() {
        return internalNode.getRaftState().getCurrentRole().isLeader();
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private String host = "127.0.0.1";
        private int port;
        private final Set<NodeAddress> peers = new HashSet<>();
        private StateMachine stateMachine;
        private final NetworkConfig config = new NetworkConfig();

        private final List<Object> services = new java.util.ArrayList<>();
        private final List<StateMachineListener> listeners = new java.util.ArrayList<>();

        public Builder localNode(String host, int port) {
            this.host = host;
            this.port = port;
            return this;
        }

        public Builder addService(Object targetService) {
            this.services.add(targetService);
            return this;
        }

        public Builder addPeers(String... peerAddresses) {
            for (String p : peerAddresses) {
                String[] parts = p.split(":");
                peers.add(new NodeAddress(parts[0], Integer.parseInt(parts[1])));
            }
            return this;
        }

        public Builder registerCommand(Class<?> commandClass) {
            this.config.registerClass(commandClass);
            return this;
        }

        public Builder addStateMachineListener(StateMachineListener listener) {
            this.listeners.add(listener);
            return this;
        }

        public KurinNode build() {
            if (port == 0 || services.isEmpty()) {
                throw new IllegalArgumentException("Port and at least one Service must be configured");
            }

            KryoSerializer serializer = new KryoSerializer(config.getRegisteredClasses());
            PojoStateMachine pojoStateMachine = new PojoStateMachine(services, serializer);

            for (StateMachineListener listener : listeners) {
                pojoStateMachine.addListener(listener);
            }

            this.stateMachine = pojoStateMachine;

            NodeAddress localAddress = new NodeAddress(host, port);
            RaftNode node = new RaftNode(localAddress, peers, stateMachine, config);
            return new KurinNode(node);
        }
    }
}
