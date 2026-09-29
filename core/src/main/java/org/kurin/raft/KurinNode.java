package org.kurin.raft;

import org.kurin.network.config.NetworkConfig;
import org.kurin.network.model.NodeAddress;
import org.kurin.raft.rpc.ClientCommandRequest;
import org.kurin.raft.state.StateMachine;
import org.kurin.raft.tools.PojoStateMachine;

import java.util.*;
import java.util.concurrent.CompletableFuture;

public class KurinNode {
    private final RaftNode internalNode;

    private KurinNode(RaftNode internalNode) {
        this.internalNode = internalNode;
    }

    public void start() {
        internalNode.start();
    }

    public void shutdown() {
        internalNode.shutdown();
    }

    public CompletableFuture<Object> submitCommand(Object command) {
        String uniqueId = UUID.randomUUID().toString();
        ClientCommandRequest request = new ClientCommandRequest(uniqueId, command);

        CompletableFuture<Object> resultFuture = new CompletableFuture<>();

        internalNode.getRaftState().getMailbox().submit(() -> {
            internalNode.getRaftState().getCurrentRole()
                    .handleClientCommand(internalNode.getRaftState(), request)
                    .whenComplete((res, ex) -> {
                        if (ex != null) resultFuture.completeExceptionally(ex);
                        else resultFuture.complete(res);
                    });
        });

        return resultFuture;
    }

    public void triggerSnapshot() {
        internalNode.getRaftState().triggerSnapshot();
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

        public Builder localNode(String host, int port) {
            this.host = host;
            this.port = port;
            return this;
        }

        public Builder addService(Object targetService) {
            this.services.add(targetService);
            return this;
        }

        public KurinNode build() {
            if (port == 0 || services.isEmpty()) {
                throw new IllegalArgumentException("Port and at least one Service must be configured");
            }

            org.kurin.network.serializer.KryoSerializer serializer =
                    new org.kurin.network.serializer.KryoSerializer(config.getRegisteredClasses());

            this.stateMachine = new org.kurin.raft.tools.PojoStateMachine(services, serializer);

            NodeAddress localAddress = new NodeAddress(host, port);
            RaftNode node = new RaftNode(localAddress, peers, stateMachine, config);
            return new KurinNode(node);
        }

        public Builder addPeers(String... peerAddresses) {
            for (String p : peerAddresses) {
                String[] parts = p.split(":");
                peers.add(new NodeAddress(parts[0], Integer.parseInt(parts[1])));
            }
            return this;
        }

        public Builder stateMachine(StateMachine stateMachine) {
            this.stateMachine = stateMachine;
            return this;
        }

        public Builder registerCommand(Class<?> commandClass) {
            this.config.registerClass(commandClass);
            return this;
        }

        public Builder fromProperties(Properties props) {
            this.host = props.getProperty("kurin.node.host", "127.0.0.1");
            this.port = Integer.parseInt(props.getProperty("kurin.node.port", "0"));

            String peersProp = props.getProperty("kurin.node.peers", "");
            if (!peersProp.isEmpty()) {
                for (String p : peersProp.split(",")) {
                    if (!p.trim().isEmpty()) {
                        String[] parts = p.trim().split(":");
                        this.peers.add(new NodeAddress(parts[0], Integer.parseInt(parts[1])));
                    }
                }
            }
            return this;
        }
    }
}
