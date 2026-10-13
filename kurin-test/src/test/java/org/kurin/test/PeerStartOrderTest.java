package org.kurin.test;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.kurin.api.exception.KurinRemoteException;
import org.kurin.network.dto.RpcResult;
import org.kurin.raft.CommandPath;
import org.kurin.raft.KurinNode;
import org.kurin.raft.RaftConfig;
import org.kurin.raft.rpc.PreVoteRequest;
import org.kurin.raft.rpc.RequestVoteRequest;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * 1.1.2: a node that started before its peer must not stay cut off from it by its own reconnection pause.
 * <p>
 * The order of events is controlled, so the outcome does not depend on timing:
 * <ol>
 * <li>The first node runs alone until its attempts to connect have failed often enough for the pause between
 * them to be at its longest, one to two seconds.</li>
 * <li>At the very moment an attempt fails, so that the whole pause lies ahead, the first node stops campaigning
 * and its peer is started. The peer is elected in a fraction of the pause.</li>
 * <li>The first node hears of its leader over the connection the leader opened. The first thing it sends by
 * itself is the command of the test.</li>
 * </ol>
 */
class PeerStartOrderTest {
    private static final String HOST = "127.0.0.1";
    // The pause before the next attempt is between one and two seconds from the seventh failure on
    private static final int FAILURES_UNTIL_THE_LONGEST_PAUSE = 7;

    private final List<KurinNode> nodes = new ArrayList<>();

    @AfterEach
    void stopNodes() {
        nodes.forEach(KurinNode::shutdown);
    }

    private KurinNode.Builder node(int port, int peerPort) {
        KurinNode.Builder builder = KurinNode.builder()
                .localNode(HOST, port)
                .bindHost(HOST)
                .addPeers(HOST + ":" + peerPort)
                .storage(RaftConfig.StorageType.MEMORY)
                .addService(new CounterService());
        // An election takes a fraction of the reconnection pause
        builder.raftConfig().setHeartbeatIntervalMs(20);
        builder.raftConfig().setElectionTimeoutMinMs(100);
        builder.raftConfig().setElectionTimeoutMaxMs(200);
        builder.networkConfig().setConnectTimeoutMs(300);
        return builder;
    }

    private KurinNode start(KurinNode.Builder builder) {
        KurinNode node = builder.build();
        nodes.add(node);
        node.start();
        return node;
    }

    @Test
    void commandOfTheNodeThatStartedFirstPassesAsSoonAsItKnowsTheLeader() throws Exception {
        List<Integer> ports = TestPorts.freePorts(2);
        int firstPort = ports.get(0);
        int secondPort = ports.get(1);

        // Counts the attempts of the first node to connect that really failed, as opposed to requests that were
        // refused because the next attempt was not due yet. Requests that waited for the same attempt fail with
        // the same exception object, so each object is one attempt.
        Set<Throwable> failedAttempts = Collections.newSetFromMap(new IdentityHashMap<>());
        CountDownLatch pauseBegan = new CountDownLatch(1);
        KurinNode first = start(node(firstPort, secondPort).transportDecorator(transport -> (destination, payload, timeout) -> {
            if (pauseBegan.getCount() == 0 && (payload instanceof PreVoteRequest || payload instanceof RequestVoteRequest)) {
                // From here on the first node does not campaign: its peer is to become the leader, and nothing
                // but the command below is to make the first node connect
                return CompletableFuture.failedFuture(new KurinRemoteException("held back by the test"));
            }
            return transport.sendRequest(destination, payload, timeout).whenComplete((value, error) -> {
                Throwable cause = error != null ? RpcResult.unwrap(error) : null;
                if (cause != null && String.valueOf(cause.getMessage()).startsWith("Cannot connect")) {
                    synchronized (failedAttempts) {
                        if (failedAttempts.add(cause) && failedAttempts.size() == FAILURES_UNTIL_THE_LONGEST_PAUSE) {
                            pauseBegan.countDown();
                        }
                    }
                }
            });
        }));

        // The attempt has just failed: the whole pause lies ahead
        assertThat(pauseBegan.await(60, TimeUnit.SECONDS)).isTrue();
        assertThat(first.commandPath()).isEqualTo(CommandPath.DOWN);
        KurinNode second = start(node(secondPort, firstPort));
        await("the first node learns its leader").atMost(Duration.ofSeconds(10)).pollInterval(Duration.ofMillis(1))
                .until(() -> second.nodeId().equals(first.status().leaderId()) && second.leadership().isPresent());

        // Hearing from the peer ended the pause; the connection itself is opened by the first request
        assertThat(first.commandPath()).isEqualTo(CommandPath.CONNECTING);
        assertThat(second.commandPath()).isEqualTo(CommandPath.UP);

        // At once, without waiting for anything else: the node is a follower of a known leader
        int value = first.<Integer>submitCommand(new CounterService.Add(5)).get(5, TimeUnit.SECONDS);
        assertThat(value).isEqualTo(5);
        assertThat(first.<Integer>submitQuery(new CounterService.GetValue()).get(5, TimeUnit.SECONDS)).isEqualTo(5);
        assertThat(first.commandPath()).isEqualTo(CommandPath.UP);
    }
}
