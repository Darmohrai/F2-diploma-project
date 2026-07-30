package org.kurin.network.tracker;

import org.kurin.network.dto.ClusterMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

public class PendingRequestsTracker {
    private static final Logger log = LoggerFactory.getLogger(PendingRequestsTracker.class);

    private final Map<Long, CompletableFuture<ClusterMessage>> pendingRequests = new ConcurrentHashMap<>();
    private final AtomicLong correlationIdGenerator = new AtomicLong(1);

    public RegisteredRequest registerRequest() {
        long id = correlationIdGenerator.getAndIncrement();
        CompletableFuture<ClusterMessage> future = new CompletableFuture<>();

        pendingRequests.put(id, future);
        return new RegisteredRequest(id, future);
    }

    public void completeRequest(long correlationId, ClusterMessage response) {
        CompletableFuture<ClusterMessage> future = pendingRequests.remove(correlationId);

        if (future != null) {
            future.complete(response);
        } else {
            log.warn("Received response for unknown or expired Correlation ID: {}", correlationId);
        }
    }

    public void failAll(Throwable reason) {
        log.error("Failing all {} pending requests due to network error", pendingRequests.size());

        for (CompletableFuture<ClusterMessage> future : pendingRequests.values()) {
            future.completeExceptionally(reason);
        }
        pendingRequests.clear();
    }

    public record RegisteredRequest(long correlationId, CompletableFuture<ClusterMessage> future) {
    }
}
