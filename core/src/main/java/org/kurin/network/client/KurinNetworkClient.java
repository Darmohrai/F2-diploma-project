package org.kurin.network.client;

import io.netty.util.Timeout;
import io.netty.util.Timer;
import org.kurin.network.dto.ClusterMessage;
import org.kurin.network.model.MessageType;
import org.kurin.network.model.NodeAddress;
import org.kurin.network.tracker.PendingRequestsTracker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class KurinNetworkClient {

    private static final Logger log = LoggerFactory.getLogger(KurinNetworkClient.class);

    private final int timeoutSeconds;

    private final ConnectionManager connectionManager;
    private final PendingRequestsTracker tracker;
    private final Timer timer;

    public KurinNetworkClient(ConnectionManager connectionManager, PendingRequestsTracker tracker, Timer timer, int timeoutSeconds) {
        this.connectionManager = connectionManager;
        this.tracker = tracker;
        this.timer = timer;
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
        CompletableFuture<ClusterMessage> responseFuture = registered.future();

        Timeout timeoutTask = timer.newTimeout(timeout -> {
            if (!responseFuture.isDone()) {
                log.warn("Request {} to {} timed out", registered.correlationId(), destination.asString());
                responseFuture.completeExceptionally(new TimeoutException("Request timed out"));
                tracker.completeRequest(registered.correlationId(), null);
            }
        }, timeoutSeconds, TimeUnit.SECONDS);

        responseFuture.whenComplete((res, ex) -> {
            if (!timeoutTask.isExpired()) timeoutTask.cancel();
        });

        return responseFuture
                .thenApply(ClusterMessage::getPayload)
                .exceptionally(ex -> {
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
