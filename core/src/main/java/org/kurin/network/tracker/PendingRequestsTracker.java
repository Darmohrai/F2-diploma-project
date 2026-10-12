package org.kurin.network.tracker;

import io.netty.channel.Channel;
import org.kurin.network.dto.RpcResult;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Requests waiting for a response, each bound to the channel it was written to.
 */
public class PendingRequestsTracker {
    private static final class Pending {
        final CompletableFuture<Object> future = new CompletableFuture<>();
        volatile Channel channel;
    }

    private final Map<Long, Pending> pendingRequests = new ConcurrentHashMap<>();
    private final AtomicLong correlationIdGenerator = new AtomicLong(1);

    public RegisteredRequest registerRequest() {
        long id = correlationIdGenerator.getAndIncrement();
        Pending pending = new Pending();
        pendingRequests.put(id, pending);
        return new RegisteredRequest(id, pending.future);
    }

    /**
     * Remembers the channel carrying the request, so the request fails as soon as that channel closes.
     */
    public void bind(long correlationId, Channel channel) {
        Pending pending = pendingRequests.get(correlationId);
        if (pending != null) {
            pending.channel = channel;
        }
    }

    public boolean isPending(long correlationId) {
        return pendingRequests.containsKey(correlationId);
    }

    public void complete(long correlationId, RpcResult result) {
        Pending pending = pendingRequests.remove(correlationId);
        if (pending == null) {
            return; // already timed out or failed
        }
        if (result.ok()) {
            pending.future.complete(result.value());
        } else {
            pending.future.completeExceptionally(result.toException());
        }
    }

    public void fail(long correlationId, Throwable reason) {
        Pending pending = pendingRequests.remove(correlationId);
        if (pending != null) {
            pending.future.completeExceptionally(reason);
        }
    }

    public void failAllOn(Channel channel, Throwable reason) {
        pendingRequests.forEach((id, pending) -> {
            if (pending.channel == channel) {
                fail(id, reason);
            }
        });
    }

    public void failAll(Throwable reason) {
        pendingRequests.keySet().forEach(id -> fail(id, reason));
    }

    public record RegisteredRequest(long correlationId, CompletableFuture<Object> future) {
    }
}
