package org.kurin.network.client;

import io.netty.channel.Channel;
import io.netty.util.Timer;
import org.kurin.api.exception.KurinException;
import org.kurin.api.exception.KurinRemoteException;
import org.kurin.api.exception.KurinSerializationException;
import org.kurin.network.dto.ClusterMessage;
import org.kurin.network.dto.RpcResult;
import org.kurin.network.model.NodeAddress;
import org.kurin.network.tracker.PendingRequestsTracker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

/**
 * Keeps one outbound connection per peer and re-establishes it with exponential backoff.
 */
public class ConnectionManager {
    private static final Logger log = LoggerFactory.getLogger(ConnectionManager.class);

    private static final long BACKOFF_MIN_MS = 50;
    private static final long BACKOFF_MAX_MS = 2000;

    private record Backoff(int failures, long retryNotBeforeNanos, long delayMs) {
    }

    private final NettyClient nettyClient;
    private final PendingRequestsTracker tracker;
    private final Timer timer;
    private final Map<NodeAddress, CompletableFuture<Channel>> connections = new ConcurrentHashMap<>();
    private final Map<NodeAddress, Backoff> backoffs = new ConcurrentHashMap<>();
    private volatile boolean closed;

    public ConnectionManager(NettyClient nettyClient, PendingRequestsTracker tracker, Timer timer) {
        this.nettyClient = nettyClient;
        this.tracker = tracker;
        this.timer = timer;
    }

    public CompletableFuture<Channel> getOrConnect(NodeAddress address) {
        if (closed) {
            return CompletableFuture.failedFuture(new KurinRemoteException("Network client is shut down"));
        }
        CompletableFuture<Channel> existing = connections.get(address);
        if (existing != null) {
            if (!existing.isDone() || (!existing.isCompletedExceptionally() && existing.join().isActive())) {
                return existing;
            }
            connections.remove(address, existing);
        }

        Backoff backoff = backoffs.get(address);
        if (backoff != null && System.nanoTime() - backoff.retryNotBeforeNanos() < 0) {
            return CompletableFuture.failedFuture(new KurinRemoteException(
                    "Node " + address.asString() + " is unreachable, next connection attempt is delayed"));
        }

        CompletableFuture<Channel> created = new CompletableFuture<>();
        CompletableFuture<Channel> raced = connections.putIfAbsent(address, created);
        if (raced != null) {
            return raced;
        }

        log.debug("Connecting to {}", address.asString());
        nettyClient.connect(address.host(), address.port()).whenComplete((channel, error) -> {
            if (error != null) {
                Backoff next = registerFailure(address);
                connections.remove(address, created);
                Throwable cause = RpcResult.unwrap(error);
                log.debug("Cannot connect to {} (attempt {}, next in {} ms): {}",
                        address.asString(), next.failures(), next.delayMs(), cause.toString());
                created.completeExceptionally(cause instanceof KurinException ? cause
                        : new KurinRemoteException("Cannot connect to " + address.asString() + ": " + cause, cause));
                return;
            }
            backoffs.remove(address);
            channel.closeFuture().addListener(closed -> {
                connections.remove(address, created);
                tracker.failAllOn(channel, new KurinRemoteException("connection closed"));
            });
            log.info("Connected to {}", address.asString());
            created.complete(channel);
        });
        return created;
    }

    private Backoff registerFailure(NodeAddress address) {
        return backoffs.compute(address, (key, previous) -> {
            int failures = previous != null ? previous.failures() + 1 : 1;
            long ceiling = Math.min(BACKOFF_MAX_MS, BACKOFF_MIN_MS << Math.min(failures - 1, 10));
            // Full jitter over the upper half keeps nodes from reconnecting in lockstep
            long delayMs = ceiling / 2 + ThreadLocalRandom.current().nextLong(ceiling / 2 + 1);
            return new Backoff(failures, System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(delayMs), delayMs);
        });
    }

    /**
     * Writes the request to the peer. Failures are reported through the tracker entry of the request.
     */
    public void send(NodeAddress destination, ClusterMessage message) {
        send(destination, message, true);
    }

    private void send(NodeAddress destination, ClusterMessage message, boolean mayRetry) {
        long correlationId = message.getCorrelationId();
        getOrConnect(destination).whenComplete((channel, connectError) -> {
            if (connectError != null) {
                tracker.fail(correlationId, RpcResult.unwrap(connectError));
                return;
            }
            tracker.bind(correlationId, channel);
            channel.writeAndFlush(message).addListener(write -> {
                if (write.isSuccess()) {
                    return;
                }
                Throwable cause = RpcResult.unwrap(write.cause());
                if (cause.getCause() instanceof KurinSerializationException serialization) {
                    cause = serialization; // unwrap Netty's EncoderException
                }
                if (cause instanceof KurinSerializationException) {
                    tracker.fail(correlationId, cause);
                } else if (mayRetry && !channel.isActive() && tracker.isPending(correlationId)) {
                    // The channel died between lookup and write: retry once on a fresh connection
                    timer.newTimeout(timeout -> send(destination, message, false), BACKOFF_MIN_MS, TimeUnit.MILLISECONDS);
                } else {
                    tracker.fail(correlationId, new KurinRemoteException(
                            "Cannot send request to " + destination.asString() + ": " + cause, cause));
                }
            });
        });
    }

    public void close() {
        closed = true;
        connections.values().forEach(connection -> connection.thenAccept(Channel::close));
        connections.clear();
        tracker.failAll(new KurinRemoteException("Network client is shut down"));
    }
}
