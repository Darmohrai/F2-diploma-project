package org.kurin.network.client;

import io.netty.channel.Channel;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.util.HashedWheelTimer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.kurin.api.exception.KurinRemoteException;
import org.kurin.api.exception.KurinTimeoutException;
import org.kurin.network.client.ConnectionManager.Reachability;
import org.kurin.network.dto.ClusterMessage;
import org.kurin.network.dto.RpcResult;
import org.kurin.network.model.NodeAddress;
import org.kurin.network.tracker.PendingRequestsTracker;
import org.kurin.raft.rpc.ClientCommandRequest;
import org.kurin.raft.rpc.ClientQueryRequest;
import org.kurin.raft.rpc.PreVoteRequest;
import org.kurin.api.ReadConsistency;

import java.net.ConnectException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

/**
 * 1.1.2: the pause between connection attempts must not keep a node from talking to a peer that is back.
 * The connections are scripted: the test decides when an attempt ends and how.
 */
class ConnectionManagerTest {
    private static final NodeAddress PEER = new NodeAddress("10.0.0.2", 9000);
    private static final Duration SOON = Duration.ofSeconds(5);
    // Long enough to be still running whenever a test looks; short enough to be waited for
    private static final long LONG_PAUSE_MS = 120_000;
    private static final long SHORT_PAUSE_MS = 60;

    /**
     * Every attempt to connect is a future that the test completes.
     */
    private static final class ScriptedConnector implements ConnectionManager.Connector {
        final List<CompletableFuture<Channel>> attempts = new CopyOnWriteArrayList<>();

        @Override
        public CompletableFuture<Channel> connect(String host, int port) {
            CompletableFuture<Channel> attempt = new CompletableFuture<>();
            attempts.add(attempt);
            return attempt;
        }

        void fail(int attempt) {
            attempts.get(attempt).completeExceptionally(new ConnectException("Connection refused"));
        }

        EmbeddedChannel succeed(int attempt) {
            EmbeddedChannel channel = new EmbeddedChannel();
            attempts.get(attempt).complete(channel);
            return channel;
        }
    }

    private final ScriptedConnector connector = new ScriptedConnector();
    private final PendingRequestsTracker tracker = new PendingRequestsTracker();
    private final HashedWheelTimer timer = new HashedWheelTimer(10, TimeUnit.MILLISECONDS);
    private ConnectionManager manager;
    private ExecutorService senders;

    @AfterEach
    void stop() {
        if (senders != null) {
            senders.shutdownNow();
        }
        if (manager != null) {
            manager.close();
        }
        timer.stop();
    }

    private ConnectionManager manager(long pauseMs) {
        manager = new ConnectionManager(connector, tracker, timer, pauseMs, pauseMs);
        return manager;
    }

    private KurinNetworkClient client(ConnectionManager connections) {
        return new KurinNetworkClient(connections, tracker, timer);
    }

    // The state the defect was about: one failed attempt, and a pause before the next
    private ConnectionManager pausedManager(long pauseMs) {
        ConnectionManager connections = manager(pauseMs);
        CompletableFuture<Channel> first = connections.getOrConnect(PEER);
        connector.fail(0);
        assertThat(first).isCompletedExceptionally();
        return connections;
    }

    private static ClientCommandRequest command(String requestId) {
        return new ClientCommandRequest(requestId, "work", null);
    }

    private static PreVoteRequest raftRequest() {
        return new PreVoteRequest(1, new NodeAddress("10.0.0.1", 9000), 0, 0);
    }

    private static Throwable failureOf(CompletableFuture<?> future) {
        try {
            future.get(5, TimeUnit.SECONDS);
        } catch (ExecutionException e) {
            return e.getCause();
        } catch (Exception e) {
            throw new AssertionError("The future did not fail in time", e);
        }
        throw new AssertionError("The future did not fail");
    }

    // ---------------------------------------------------------------- the pause

    @Test
    void failedAttemptNamesItsCauseAndStartsAPause() {
        ConnectionManager connections = manager(LONG_PAUSE_MS);

        CompletableFuture<Channel> first = connections.getOrConnect(PEER);
        assertThat(connections.reachability(PEER)).isEqualTo(Reachability.CONNECTING);
        connector.fail(0);

        assertThat(failureOf(first)).isInstanceOf(KurinRemoteException.class)
                .hasMessageContaining("Cannot connect to 10.0.0.2:9000")
                .hasCauseInstanceOf(ConnectException.class);
        assertThat(connections.reachability(PEER)).isEqualTo(Reachability.PAUSED);
    }

    @Test
    void duringThePauseARequestOfTheRaftProtocolFailsAtOnceWithoutAnAttempt() {
        ConnectionManager connections = pausedManager(LONG_PAUSE_MS);

        assertThat(failureOf(connections.getOrConnect(PEER))).isInstanceOf(KurinRemoteException.class)
                .hasMessageContaining("next connection attempt is delayed");
        assertThat(failureOf(client(connections).sendRequest(PEER, raftRequest(), Duration.ofSeconds(30))))
                .isInstanceOf(KurinRemoteException.class)
                .hasMessageContaining("next connection attempt is delayed");
        assertThat(connector.attempts).hasSize(1);
    }

    @Test
    void connectorThatThrowsCountsAsAFailedAttempt() {
        manager = new ConnectionManager((host, port) -> {
            throw new IllegalStateException("no route");
        }, tracker, timer, LONG_PAUSE_MS, LONG_PAUSE_MS);

        assertThat(failureOf(manager.getOrConnect(PEER))).isInstanceOf(KurinRemoteException.class).hasMessageContaining("no route");
        assertThat(manager.reachability(PEER)).isEqualTo(Reachability.PAUSED);
    }

    // ---------------------------------------------------------------- signs of life

    @Test
    void signOfLifeLiftsThePauseAndTheNextRequestConnectsAtOnce() {
        ConnectionManager connections = pausedManager(LONG_PAUSE_MS);

        connections.peerAlive(PEER);

        assertThat(connections.reachability(PEER)).isEqualTo(Reachability.NOT_CONNECTED);
        CompletableFuture<Channel> second = connections.getOrConnect(PEER);
        assertThat(connector.attempts).hasSize(2);
        assertThat(connections.reachability(PEER)).isEqualTo(Reachability.CONNECTING);

        EmbeddedChannel channel = connector.succeed(1);
        assertThat(second.join()).isSameAs(channel);
        assertThat(connections.reachability(PEER)).isEqualTo(Reachability.CONNECTED);
        // The open connection is used from now on
        assertThat(connections.getOrConnect(PEER).join()).isSameAs(channel);
        assertThat(connector.attempts).hasSize(2);
    }

    @Test
    void signOfLifeWithoutAPauseChangesNothing() {
        ConnectionManager connections = manager(LONG_PAUSE_MS);

        connections.peerAlive(PEER);

        assertThat(connections.reachability(PEER)).isEqualTo(Reachability.NOT_CONNECTED);
        assertThat(connector.attempts).isEmpty();
    }

    @Test
    void peerThatTalksButCannotBeReachedCostsOneExtraAttemptNotOnePerMessage() {
        ConnectionManager connections = pausedManager(LONG_PAUSE_MS);
        connections.peerAlive(PEER);
        CompletableFuture<Channel> second = connections.getOrConnect(PEER);
        connector.fail(1);
        assertThat(second).isCompletedExceptionally();

        // The peer keeps talking: a hundred heartbeats, not one more attempt
        for (int heartbeat = 0; heartbeat < 100; heartbeat++) {
            connections.peerAlive(PEER);
            assertThat(connections.getOrConnect(PEER)).isCompletedExceptionally();
        }

        assertThat(connections.reachability(PEER)).isEqualTo(Reachability.PAUSED);
        assertThat(connector.attempts).hasSize(2);
    }

    @Test
    void afterASuccessfulConnectionASignOfLifeLiftsAPauseAgain() {
        ConnectionManager connections = pausedManager(LONG_PAUSE_MS);
        connections.peerAlive(PEER);
        connections.getOrConnect(PEER);
        EmbeddedChannel channel = connector.succeed(1);

        // The peer goes away and the next attempt fails
        channel.close();
        assertThat(connections.reachability(PEER)).isEqualTo(Reachability.NOT_CONNECTED);
        connections.getOrConnect(PEER);
        connector.fail(2);
        assertThat(connections.reachability(PEER)).isEqualTo(Reachability.PAUSED);

        connections.peerAlive(PEER);

        assertThat(connections.reachability(PEER)).isEqualTo(Reachability.NOT_CONNECTED);
        connections.getOrConnect(PEER);
        assertThat(connector.attempts).hasSize(4);
    }

    @Test
    void failureOfAnAttemptThatBeganBeforeTheSignOfLifeIsFollowedByTheAttemptItAllows() {
        ConnectionManager connections = pausedManager(SHORT_PAUSE_MS);
        await().atMost(SOON).until(() -> connections.reachability(PEER) == Reachability.NOT_CONNECTED);
        // The pause is over and an attempt is under way when the peer comes up and says so
        CompletableFuture<Channel> underWay = connections.getOrConnect(PEER);
        assertThat(connector.attempts).hasSize(2);
        connections.peerAlive(PEER);

        // That attempt was made against a peer that was not there yet
        connector.fail(1);

        // Nobody is told: the attempt against the peer that is there now decides
        assertThat(connector.attempts).hasSize(3);
        assertThat(underWay).isNotDone();
        EmbeddedChannel channel = connector.succeed(2);
        assertThat(underWay.join()).isSameAs(channel);
    }

    @Test
    void whenTheAttemptAllowedByTheSignOfLifeFailsTooItsFailureIsReported() {
        ConnectionManager connections = pausedManager(SHORT_PAUSE_MS);
        await().atMost(SOON).until(() -> connections.reachability(PEER) == Reachability.NOT_CONNECTED);
        CompletableFuture<Channel> underWay = connections.getOrConnect(PEER);
        connections.peerAlive(PEER);
        connector.fail(1);

        connector.fail(2);

        assertThat(failureOf(underWay)).isInstanceOf(KurinRemoteException.class).hasMessageContaining("Cannot connect");
        assertThat(connector.attempts).hasSize(3);
    }

    // ---------------------------------------------------------------- requests that carry the work of a client

    @Test
    void forwardedRequestWaitsForTheNextAttemptInsteadOfFailingAtOnce() throws Exception {
        ConnectionManager connections = pausedManager(LONG_PAUSE_MS);
        KurinNetworkClient client = client(connections);

        CompletableFuture<Object> command = client.sendRequest(PEER, command("r-1"), Duration.ofSeconds(30));
        CompletableFuture<Object> query = client.sendRequest(PEER,
                new ClientQueryRequest("r-2", "question", ReadConsistency.LINEARIZABLE, null), Duration.ofSeconds(30));

        assertThat(command).isNotDone();
        assertThat(query).isNotDone();
        assertThat(connector.attempts).hasSize(1);

        // The leader is heard from: the attempt is made now, and both requests go out over it
        connections.peerAlive(PEER);
        assertThat(connector.attempts).hasSize(2);
        EmbeddedChannel channel = connector.succeed(1);

        ClusterMessage first = channel.readOutbound();
        ClusterMessage second = channel.readOutbound();
        assertThat(List.of(first.getPayload(), second.getPayload())).extracting(payload -> payload.getClass().getSimpleName())
                .containsExactlyInAnyOrder("ClientCommandRequest", "ClientQueryRequest");
        ClusterMessage sentCommand = first.getPayload() instanceof ClientCommandRequest ? first : second;
        tracker.complete(sentCommand.getCorrelationId(), RpcResult.success("applied"));
        assertThat(command.get(5, TimeUnit.SECONDS)).isEqualTo("applied");
    }

    @Test
    void forwardedRequestWaitsOutThePauseWhenThePeerStaysSilent() {
        ConnectionManager connections = pausedManager(SHORT_PAUSE_MS);

        CompletableFuture<Object> command = client(connections).sendRequest(PEER, command("r-1"), Duration.ofSeconds(30));

        // No sign of life: the attempt comes when the pause is over
        await().atMost(SOON).until(() -> connector.attempts.size() == 2);
        EmbeddedChannel channel = connector.succeed(1);
        ClusterMessage sent = channel.readOutbound();
        assertThat(sent.getPayload()).isInstanceOf(ClientCommandRequest.class);
        assertThat(command).isNotDone();
    }

    @Test
    void forwardedRequestFailsWithTheCauseOnlyWhenTheAttemptItWaitedForFails() {
        ConnectionManager connections = pausedManager(LONG_PAUSE_MS);
        CompletableFuture<Object> command = client(connections).sendRequest(PEER, command("r-1"), Duration.ofSeconds(30));
        connections.peerAlive(PEER);
        assertThat(command).isNotDone();

        connector.fail(1);

        assertThat(failureOf(command)).isInstanceOf(KurinRemoteException.class)
                .hasMessageContaining("Cannot connect to 10.0.0.2:9000")
                .hasCauseInstanceOf(ConnectException.class);
    }

    @Test
    void forwardedRequestGivesUpAtItsOwnTimeoutAndIsNotSentAfterwards() {
        ConnectionManager connections = pausedManager(LONG_PAUSE_MS);

        CompletableFuture<Object> command = client(connections).sendRequest(PEER, command("r-1"), Duration.ofMillis(100));

        assertThat(failureOf(command)).isInstanceOf(KurinTimeoutException.class).hasMessageContaining("within 100 ms");
        connections.peerAlive(PEER);
        EmbeddedChannel channel = connector.succeed(1);
        assertThat((Object) channel.readOutbound()).isNull();
    }

    @Test
    void parallelForwardsToOnePeerShareOneConnectionAttempt() throws Exception {
        ConnectionManager connections = pausedManager(LONG_PAUSE_MS);
        KurinNetworkClient client = client(connections);
        int requests = 64;
        senders = Executors.newFixedThreadPool(8);
        CountDownLatch go = new CountDownLatch(1);

        List<Future<CompletableFuture<Object>>> sending = new ArrayList<>();
        for (int i = 0; i < requests; i++) {
            String requestId = "r-" + i;
            sending.add(senders.submit(() -> {
                go.await();
                return client.sendRequest(PEER, command(requestId), Duration.ofSeconds(30));
            }));
        }
        go.countDown();
        List<CompletableFuture<Object>> results = new ArrayList<>();
        for (Future<CompletableFuture<Object>> sent : sending) {
            results.add(sent.get(5, TimeUnit.SECONDS));
        }

        // All of them wait, and none has caused an attempt of its own
        assertThat(results).allSatisfy(result -> assertThat(result).isNotDone());
        assertThat(connector.attempts).hasSize(1);

        connections.peerAlive(PEER);
        assertThat(connector.attempts).hasSize(2);
        EmbeddedChannel channel = connector.succeed(1);

        assertThat(channel.outboundMessages()).hasSize(requests);
        assertThat(connector.attempts).hasSize(2);
    }

    @Test
    void parallelRequestsWithoutAConnectionShareOneAttemptToo() throws Exception {
        ConnectionManager connections = manager(LONG_PAUSE_MS);
        KurinNetworkClient client = client(connections);
        int requests = 64;
        senders = Executors.newFixedThreadPool(8);
        CountDownLatch go = new CountDownLatch(1);

        List<Future<CompletableFuture<Object>>> sending = new ArrayList<>();
        for (int i = 0; i < requests; i++) {
            Object payload = i % 2 == 0 ? command("r-" + i) : raftRequest();
            sending.add(senders.submit(() -> {
                go.await();
                return client.sendRequest(PEER, payload, Duration.ofSeconds(30));
            }));
        }
        go.countDown();
        for (Future<CompletableFuture<Object>> sent : sending) {
            sent.get(5, TimeUnit.SECONDS);
        }

        assertThat(connector.attempts).hasSize(1);
        EmbeddedChannel channel = connector.succeed(0);
        assertThat(channel.outboundMessages()).hasSize(requests);
    }

    // ---------------------------------------------------------------- shutdown

    @Test
    void closingFailsRequestsThatWaitForAnAttempt() {
        ConnectionManager connections = pausedManager(LONG_PAUSE_MS);
        CompletableFuture<Object> command = client(connections).sendRequest(PEER, command("r-1"), Duration.ofSeconds(30));
        CompletableFuture<Channel> waiting = connections.getOrConnect(PEER, true);

        connections.close();

        assertThat(failureOf(command)).isInstanceOf(KurinRemoteException.class).hasMessageContaining("shut down");
        assertThat(failureOf(waiting)).isInstanceOf(KurinRemoteException.class).hasMessageContaining("shut down");
        assertThat(failureOf(connections.getOrConnect(PEER, true))).isInstanceOf(KurinRemoteException.class).hasMessageContaining("shut down");
        // The sign of life of a peer that is still up finds nothing to start
        connections.peerAlive(PEER);
        assertThat(connector.attempts).hasSize(1);
    }

    @Test
    void waitingForAnAttemptAfterTheTimerStoppedFailsInsteadOfHanging() {
        ConnectionManager connections = pausedManager(LONG_PAUSE_MS);
        timer.stop();

        assertThatThrownBy(() -> connections.getOrConnect(PEER, true).get(5, TimeUnit.SECONDS))
                .hasCauseInstanceOf(KurinRemoteException.class)
                .hasMessageContaining("shut down");
    }
}
