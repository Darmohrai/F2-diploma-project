package org.kurin.test;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.kurin.api.exception.KurinRemoteException;
import org.kurin.network.client.KurinTransport;
import org.kurin.raft.KurinNode;
import org.kurin.raft.RaftConfig;
import org.kurin.raft.rpc.RequestVoteRequest;

import java.net.ServerSocket;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class HandshakeTest {

    public record ExtraType(String value) {
    }

    private final List<KurinNode> nodes = new ArrayList<>();

    @AfterEach
    void stopNodes() {
        nodes.forEach(KurinNode::shutdown);
    }

    private static int freePort() throws Exception {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }

    private KurinNode startNode(int port, int peerPort, AtomicReference<KurinTransport> transport, Consumer<KurinNode.Builder> customizer) {
        KurinNode.Builder builder = KurinNode.builder()
                .localNode("127.0.0.1", port)
                .bindHost("127.0.0.1")
                .addPeers("127.0.0.1:" + peerPort)
                .storage(RaftConfig.StorageType.MEMORY)
                .addService(new CounterService())
                .transportDecorator(real -> {
                    transport.set(real);
                    return real;
                });
        customizer.accept(builder);
        KurinNode node = builder.build();
        node.start();
        nodes.add(node);
        return node;
    }

    private void assertRejected(Consumer<KurinNode.Builder> first, Consumer<KurinNode.Builder> second, String expectedReason) throws Exception {
        int firstPort = freePort();
        int secondPort = freePort();
        AtomicReference<KurinTransport> transport = new AtomicReference<>();
        KurinNode firstNode = startNode(firstPort, secondPort, transport, first);
        KurinNode secondNode = startNode(secondPort, firstPort, new AtomicReference<>(), second);

        RequestVoteRequest probe = new RequestVoteRequest(0, firstNode.getLocalAddress(), 0, 0);
        assertThatThrownBy(() -> transport.get().sendRequest(secondNode.getLocalAddress(), probe, Duration.ofSeconds(2)).get(5, TimeUnit.SECONDS))
                .isInstanceOf(ExecutionException.class)
                .cause()
                .isInstanceOf(KurinRemoteException.class)
                .hasMessageContaining(expectedReason);

        assertThat(firstNode.isLeader()).isFalse();
        assertThat(secondNode.isLeader()).isFalse();
    }

    // Spec test 9
    @Test
    void nodesWithDifferentTypeRegistriesRefuseToConnect() throws Exception {
        assertRejected(builder -> {
        }, builder -> builder.registerType(ExtraType.class), "type registry mismatch");
    }

    @Test
    void nodesOfDifferentClustersRefuseToConnect() throws Exception {
        assertRejected(builder -> builder.clusterName("orders"), builder -> builder.clusterName("billing"), "cluster name mismatch");
    }

    @Test
    void nodesWithDifferentSecretsRefuseToConnect() throws Exception {
        assertRejected(builder -> builder.networkConfig().setClusterSecret("alpha"),
                builder -> builder.networkConfig().setClusterSecret("beta"), "cluster secret mismatch");
    }

    @Test
    void nodesWithTheSameSecretFormACluster() throws Exception {
        int firstPort = freePort();
        int secondPort = freePort();
        KurinNode first = startNode(firstPort, secondPort, new AtomicReference<>(), builder -> fast(builder).networkConfig().setClusterSecret("shared"));
        KurinNode second = startNode(secondPort, firstPort, new AtomicReference<>(), builder -> fast(builder).networkConfig().setClusterSecret("shared"));

        org.awaitility.Awaitility.await().atMost(Duration.ofSeconds(10))
                .until(() -> first.isLeader() || second.isLeader());
    }

    private static KurinNode.Builder fast(KurinNode.Builder builder) {
        builder.raftConfig().setHeartbeatIntervalMs(30);
        builder.raftConfig().setElectionTimeoutMinMs(300);
        builder.raftConfig().setElectionTimeoutMaxMs(600);
        return builder;
    }
}
