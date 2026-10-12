package org.kurin.test;

import org.kurin.api.exception.KurinTimeoutException;
import org.kurin.network.client.KurinTransport;
import org.kurin.network.model.NodeAddress;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

/**
 * Wraps the real Netty transport of a node and loses or delays outgoing requests according to
 * {@link NetworkFaults}. A lost request behaves like a packet dropped by the network: the caller sees
 * a timeout, never an immediate error.
 */
public final class FaultInjectingTransport implements KurinTransport {
    private final String localNodeId;
    private final KurinTransport delegate;
    private final NetworkFaults faults;
    private final ScheduledExecutorService scheduler;

    public FaultInjectingTransport(String localNodeId, KurinTransport delegate, NetworkFaults faults,
                                   ScheduledExecutorService scheduler) {
        this.localNodeId = localNodeId;
        this.delegate = delegate;
        this.faults = faults;
        this.scheduler = scheduler;
    }

    @Override
    public CompletableFuture<Object> sendRequest(NodeAddress destination, Object payload, Duration timeout) {
        String remoteNodeId = destination.asString();

        double dropRate = faults.dropRateOf(localNodeId, remoteNodeId);
        boolean dropped = faults.isBlocked(localNodeId, remoteNodeId)
                || (dropRate > 0 && ThreadLocalRandom.current().nextDouble() < dropRate);
        if (dropped) {
            CompletableFuture<Object> lost = new CompletableFuture<>();
            scheduler.schedule(() -> lost.completeExceptionally(new KurinTimeoutException(
                            "No response from " + remoteNodeId + " within " + timeout.toMillis() + " ms (dropped by fault injection)")),
                    timeout.toMillis(), TimeUnit.MILLISECONDS);
            return lost;
        }

        Duration delay = faults.delayOf(localNodeId, remoteNodeId);
        if (delay.isZero()) {
            return delegate.sendRequest(destination, payload, timeout);
        }

        CompletableFuture<Object> delayed = new CompletableFuture<>();
        scheduler.schedule(() -> {
            // The link may have been cut while the message was "on the wire"
            if (faults.isBlocked(localNodeId, remoteNodeId)) {
                delayed.completeExceptionally(new KurinTimeoutException(
                        "No response from " + remoteNodeId + " (dropped by fault injection)"));
                return;
            }
            delegate.sendRequest(destination, payload, timeout).whenComplete((value, error) -> {
                if (error != null) {
                    delayed.completeExceptionally(error);
                } else {
                    delayed.complete(value);
                }
            });
        }, delay.toMillis(), TimeUnit.MILLISECONDS);
        return delayed;
    }
}
