package org.kurin.network.dispatcher;

import org.kurin.network.dto.ClusterMessage;

import java.util.concurrent.CompletableFuture;

public interface MessageDispatcher {
    CompletableFuture<Object> dispatchAsync(ClusterMessage request);
}
