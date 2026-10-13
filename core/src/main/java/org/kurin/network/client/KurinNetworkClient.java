package org.kurin.network.client;

import io.netty.util.Timeout;
import io.netty.util.Timer;
import org.kurin.api.exception.KurinTimeoutException;
import org.kurin.network.dto.ClusterMessage;
import org.kurin.network.model.MessageType;
import org.kurin.network.model.NodeAddress;
import org.kurin.network.tracker.PendingRequestsTracker;
import org.kurin.raft.rpc.ClientCommandRequest;
import org.kurin.raft.rpc.ClientQueryRequest;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

public class KurinNetworkClient implements KurinTransport {
    private final ConnectionManager connectionManager;
    private final PendingRequestsTracker tracker;
    private final Timer timer;

    public KurinNetworkClient(ConnectionManager connectionManager, PendingRequestsTracker tracker, Timer timer) {
        this.connectionManager = connectionManager;
        this.tracker = tracker;
        this.timer = timer;
    }

    @Override
    public CompletableFuture<Object> sendRequest(NodeAddress destination, Object payload, Duration timeout) {
        PendingRequestsTracker.RegisteredRequest registered = tracker.registerRequest();
        long correlationId = registered.correlationId();

        Timeout timeoutTask = timer.newTimeout(expired -> tracker.fail(correlationId, new KurinTimeoutException(
                        "No response from " + destination.asString() + " within " + timeout.toMillis() + " ms")),
                timeout.toMillis(), TimeUnit.MILLISECONDS);
        registered.future().whenComplete((result, error) -> timeoutTask.cancel());

        connectionManager.send(destination, new ClusterMessage(MessageType.REQUEST, correlationId, payload),
                carriesClientWork(payload));
        return registered.future();
    }

    /**
     * A command or query on its way to the leader is somebody's request: it waits for a connection for as long
     * as it may wait at all. Everything else is the Raft protocol talking to itself; its timers send it again,
     * so it fails fast while a peer is out of reach.
     */
    private static boolean carriesClientWork(Object payload) {
        return payload instanceof ClientCommandRequest || payload instanceof ClientQueryRequest;
    }

    /**
     * A valid message of the peer arrived over a connection that the peer opened.
     *
     * @see ConnectionManager#peerAlive
     */
    public void peerAlive(NodeAddress peer) {
        connectionManager.peerAlive(peer);
    }

    public ConnectionManager.Reachability reachability(NodeAddress peer) {
        return connectionManager.reachability(peer);
    }

    public void close() {
        connectionManager.close();
    }
}
