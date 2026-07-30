package org.kurin.network.client;

import org.kurin.network.dto.ClusterMessage;
import org.kurin.network.model.MessageType;
import org.kurin.network.model.NodeAddress;
import org.kurin.network.tracker.PendingRequestsTracker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

public class KurinNetworkClient {

    private static final Logger log = LoggerFactory.getLogger(KurinNetworkClient.class);

    private final int timeoutSeconds;

    private final ConnectionManager connectionManager;
    private final PendingRequestsTracker tracker;

    public KurinNetworkClient(ConnectionManager connectionManager, PendingRequestsTracker tracker, int timeoutSeconds) {
        this.connectionManager = connectionManager;
        this.tracker = tracker;
        this.timeoutSeconds = timeoutSeconds;
    }

    public CompletableFuture<Object> sendRequest(NodeAddress destination, Object payload) {

        PendingRequestsTracker.RegisteredRequest registered = tracker.registerRequest();

        ClusterMessage message = new ClusterMessage(
                MessageType.REQUEST,
                registered.correlationId(),
                payload
        );

        connectionManager.sendAsync(destination, message);

        return registered.future()
                .orTimeout(timeoutSeconds, TimeUnit.SECONDS)
                .thenApply(ClusterMessage::getPayload)
                .exceptionally(ex -> {
                    log.error("Request {} to {} failed: {}",
                            registered.correlationId(), destination.asString(), ex.getMessage());
                    throw new RuntimeException("Network request failed", ex);
                });
    }

    public void sendOneWay(NodeAddress destination, Object payload) {
        long dummyId = -1L;

        ClusterMessage message = new ClusterMessage(
                MessageType.REQUEST,
                dummyId,
                payload
        );

        connectionManager.sendAsync(destination, message);
    }
}
