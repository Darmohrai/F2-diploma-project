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
 * <p>
 * After a failed attempt the next one is not made before a pause is over. What happens to a request in that
 * pause depends on who sends it:
 * <ul>
 * <li>Requests of the Raft protocol itself (heartbeats, votes) fail at once. They are repeated by their timers
 * anyway, and failing fast keeps a dead peer from costing one connection attempt per heartbeat.</li>
 * <li>Requests that carry the work of a client (a command or query forwarded to the leader) wait for the next
 * attempt, within their own timeout, and fail only if that attempt fails. All of them share that one attempt.</li>
 * </ul>
 * A pause is a guess that the peer is still away. When the peer is heard from over a connection it opened itself,
 * the guess is refuted and the pause is lifted ({@link #peerAlive}). That happens once: if the attempt that
 * follows fails as well, the peer is alive but cannot be reached from here, and pauses are served in full until a
 * connection succeeds. So a peer that talks without being reachable costs one extra attempt, not one per message.
 * <p>
 * Nothing here runs on the Raft thread: callers are the I/O threads, the timer and whoever sends a request.
 */
public class ConnectionManager {
    private static final Logger log = LoggerFactory.getLogger(ConnectionManager.class);

    private static final long BACKOFF_MIN_MS = 50;
    private static final long BACKOFF_MAX_MS = 2000;

    /**
     * Opens a connection to a peer, handshake included.
     */
    public interface Connector {
        CompletableFuture<Channel> connect(String host, int port);
    }

    /**
     * What is known about the outbound path to one peer.
     */
    public enum Reachability {
        /**
         * A connection is open.
         */
        CONNECTED,
        /**
         * A connection is being opened.
         */
        CONNECTING,
        /**
         * No connection is open and nothing stands in the way of opening one: the next request does it.
         * Idle connections are closed, so this is the normal state towards a peer that is not written to.
         */
        NOT_CONNECTED,
        /**
         * The last attempt failed and the next one is not due yet.
         */
        PAUSED
    }

    /**
     * @param liftedAtNanos when a sign of life lifted a pause since the last successful connection; 0 if none did
     */
    private record Backoff(int failures, long retryNotBeforeNanos, long delayMs, long liftedAtNanos) {
        boolean lifted() {
            return liftedAtNanos != 0;
        }
    }

    private final Connector connector;
    private final PendingRequestsTracker tracker;
    private final Timer timer;
    private final long backoffMinMs;
    private final long backoffMaxMs;
    private final Map<NodeAddress, CompletableFuture<Channel>> connections = new ConcurrentHashMap<>();
    private final Map<NodeAddress, Backoff> backoffs = new ConcurrentHashMap<>();
    // Requests waiting for the attempt that follows the pause, one future per peer
    private final Map<NodeAddress, CompletableFuture<Channel>> awaitedAttempts = new ConcurrentHashMap<>();
    private volatile boolean closed;

    public ConnectionManager(NettyClient nettyClient, PendingRequestsTracker tracker, Timer timer) {
        this(nettyClient::connect, tracker, timer, BACKOFF_MIN_MS, BACKOFF_MAX_MS);
    }

    /**
     * @param backoffMinMs upper bound of the pause after the first failure; it doubles with every further one
     * @param backoffMaxMs upper bound of any pause
     */
    public ConnectionManager(Connector connector, PendingRequestsTracker tracker, Timer timer, long backoffMinMs, long backoffMaxMs) {
        this.connector = connector;
        this.tracker = tracker;
        this.timer = timer;
        this.backoffMinMs = backoffMinMs;
        this.backoffMaxMs = backoffMaxMs;
    }

    /**
     * @return the open connection or the attempt under way; a failed future while the peer is being paused for
     */
    public CompletableFuture<Channel> getOrConnect(NodeAddress address) {
        return getOrConnect(address, false);
    }

    /**
     * @param awaitNextAttempt during a pause, wait for the attempt that follows it instead of failing at once
     */
    public CompletableFuture<Channel> getOrConnect(NodeAddress address, boolean awaitNextAttempt) {
        if (closed) {
            return CompletableFuture.failedFuture(new KurinRemoteException("Network client is shut down"));
        }
        CompletableFuture<Channel> usable = usableConnection(address);
        if (usable != null) {
            return usable;
        }
        Backoff backoff = backoffs.get(address);
        long pauseNanos = backoff != null ? backoff.retryNotBeforeNanos() - System.nanoTime() : 0;
        if (pauseNanos <= 0) {
            return connect(address);
        }
        if (!awaitNextAttempt) {
            return CompletableFuture.failedFuture(new KurinRemoteException(
                    "Node " + address.asString() + " is unreachable, next connection attempt is delayed"));
        }
        return awaitAttemptAfterPause(address, pauseNanos);
    }

    // The open connection or the attempt under way; null if there is neither
    private CompletableFuture<Channel> usableConnection(NodeAddress address) {
        CompletableFuture<Channel> existing = connections.get(address);
        if (existing == null) {
            return null;
        }
        if (!existing.isDone() || (!existing.isCompletedExceptionally() && existing.join().isActive())) {
            return existing;
        }
        connections.remove(address, existing);
        return null;
    }

    // One attempt per peer at a time: whoever comes while it runs gets the same future
    private CompletableFuture<Channel> connect(NodeAddress address) {
        CompletableFuture<Channel> usable = usableConnection(address);
        if (usable != null) {
            return usable;
        }
        CompletableFuture<Channel> created = new CompletableFuture<>();
        CompletableFuture<Channel> raced = connections.putIfAbsent(address, created);
        if (raced != null) {
            return raced;
        }

        log.debug("Connecting to {}", address.asString());
        long startedNanos = System.nanoTime();
        CompletableFuture<Channel> attempt;
        try {
            attempt = connector.connect(address.host(), address.port());
        } catch (RuntimeException e) {
            attempt = CompletableFuture.failedFuture(e);
        }
        attempt.whenComplete((channel, error) -> {
            if (error != null) {
                Failure failure = registerFailure(address, startedNanos);
                connections.remove(address, created);
                Throwable cause = RpcResult.unwrap(error);
                if (failure.beganBeforeSignOfLife() && !closed) {
                    // The peer was heard from while this attempt was under way. Whoever waits for the attempt
                    // gets the result of the one that the sign of life allows, which is made now.
                    log.debug("Cannot connect to {} ({}), but it was heard from in the meantime: connecting again",
                            address.asString(), cause.toString());
                    relay(connect(address), created);
                    return;
                }
                log.debug("Cannot connect to {} (attempt {}, next in {} ms): {}",
                        address.asString(), failure.backoff().failures(), failure.backoff().delayMs(), cause.toString());
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

    private record Failure(Backoff backoff, boolean beganBeforeSignOfLife) {
    }

    private Failure registerFailure(NodeAddress address, long attemptStartedNanos) {
        boolean[] beganBeforeSignOfLife = {false};
        Backoff next = backoffs.compute(address, (key, previous) -> {
            int failures = previous != null ? previous.failures() + 1 : 1;
            long liftedAtNanos = previous != null ? previous.liftedAtNanos() : 0;
            long now = System.nanoTime();
            if (liftedAtNanos != 0 && attemptStartedNanos - liftedAtNanos < 0) {
                // This attempt began before the peer was heard from: its failure says nothing about the peer
                // as it is now, and the attempt the sign of life allows is still to be made
                beganBeforeSignOfLife[0] = true;
                return new Backoff(failures, now, 0, liftedAtNanos);
            }
            beganBeforeSignOfLife[0] = false;
            long ceiling = Math.min(backoffMaxMs, backoffMinMs << Math.min(failures - 1, 10));
            // Full jitter over the upper half keeps nodes from reconnecting in lockstep
            long delayMs = ceiling / 2 + ThreadLocalRandom.current().nextLong(ceiling / 2 + 1);
            return new Backoff(failures, now + TimeUnit.MILLISECONDS.toNanos(delayMs), delayMs, liftedAtNanos);
        });
        return new Failure(next, beganBeforeSignOfLife[0]);
    }

    private static void relay(CompletableFuture<Channel> from, CompletableFuture<Channel> to) {
        from.whenComplete((channel, error) -> {
            if (error != null) {
                to.completeExceptionally(RpcResult.unwrap(error));
            } else {
                to.complete(channel);
            }
        });
    }

    private CompletableFuture<Channel> awaitAttemptAfterPause(NodeAddress address, long pauseNanos) {
        CompletableFuture<Channel> waiting = new CompletableFuture<>();
        CompletableFuture<Channel> raced = awaitedAttempts.putIfAbsent(address, waiting);
        if (raced != null) {
            return raced;
        }
        try {
            timer.newTimeout(timeout -> startAwaitedAttempt(address), pauseNanos, TimeUnit.NANOSECONDS);
        } catch (IllegalStateException | java.util.concurrent.RejectedExecutionException stopped) {
            awaitedAttempts.remove(address, waiting);
            waiting.completeExceptionally(new KurinRemoteException("Network client is shut down"));
            return waiting;
        }
        if (closed && awaitedAttempts.remove(address, waiting)) {
            waiting.completeExceptionally(new KurinRemoteException("Network client is shut down"));
        }
        Backoff current = backoffs.get(address);
        if (current == null || current.retryNotBeforeNanos() - System.nanoTime() <= 0) {
            // The pause ended or was lifted between the look at it and the registration of this wait
            startAwaitedAttempt(address);
        }
        return waiting;
    }

    // Called when the pause is over and when it is lifted; whichever comes first makes the attempt
    private void startAwaitedAttempt(NodeAddress address) {
        CompletableFuture<Channel> waiting = awaitedAttempts.remove(address);
        if (waiting != null) {
            relay(connect(address), waiting);
        }
    }

    /**
     * Tells the manager that a valid message of the peer has just arrived over a connection the peer opened. If
     * outbound attempts to that peer are being paused, the pause is lifted: the next request connects at once,
     * and requests already waiting for the next attempt get it now.
     * <p>
     * Called for every inbound request, so the common case, no pause, is a single map lookup.
     */
    public void peerAlive(NodeAddress address) {
        Backoff current = backoffs.get(address);
        if (current == null || current.lifted()) {
            return;
        }
        long now = System.nanoTime();
        // Zero means "not lifted"; the clock may show exactly that
        Backoff lifted = new Backoff(current.failures(), now, 0, now != 0 ? now : 1);
        if (backoffs.replace(address, current, lifted)) {
            log.debug("Heard from {} after {} failed connection attempt(s): connecting again without waiting",
                    address.asString(), current.failures());
            startAwaitedAttempt(address);
        }
    }

    public Reachability reachability(NodeAddress address) {
        CompletableFuture<Channel> connection = connections.get(address);
        if (connection != null) {
            if (!connection.isDone()) {
                return Reachability.CONNECTING;
            }
            if (!connection.isCompletedExceptionally() && connection.join().isActive()) {
                return Reachability.CONNECTED;
            }
        }
        Backoff backoff = backoffs.get(address);
        if (backoff != null && backoff.retryNotBeforeNanos() - System.nanoTime() > 0) {
            return Reachability.PAUSED;
        }
        return Reachability.NOT_CONNECTED;
    }

    /**
     * Writes the request to the peer. Failures are reported through the tracker entry of the request.
     * During a pause after a failed connection attempt the request fails at once.
     */
    public void send(NodeAddress destination, ClusterMessage message) {
        send(destination, message, false);
    }

    /**
     * @param awaitNextAttempt during a pause after a failed connection attempt, keep the request until the next
     *                         attempt was made, and fail it only if that attempt fails or its own timeout ends
     */
    public void send(NodeAddress destination, ClusterMessage message, boolean awaitNextAttempt) {
        send(destination, message, awaitNextAttempt, true);
    }

    private void send(NodeAddress destination, ClusterMessage message, boolean awaitNextAttempt, boolean mayRetry) {
        long correlationId = message.getCorrelationId();
        getOrConnect(destination, awaitNextAttempt).whenComplete((channel, connectError) -> {
            if (connectError != null) {
                tracker.fail(correlationId, RpcResult.unwrap(connectError));
                return;
            }
            if (!tracker.isPending(correlationId)) {
                return; // timed out while it waited for the connection
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
                    timer.newTimeout(timeout -> send(destination, message, awaitNextAttempt, false), backoffMinMs, TimeUnit.MILLISECONDS);
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
        KurinRemoteException shutDown = new KurinRemoteException("Network client is shut down");
        awaitedAttempts.values().forEach(waiting -> waiting.completeExceptionally(shutDown));
        awaitedAttempts.clear();
        tracker.failAll(shutDown);
    }
}
