package org.kurin.network.client;

import org.kurin.network.model.NodeAddress;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;

/**
 * Request/response channel to other nodes. The Raft core talks to peers only through this interface,
 * which lets tests wrap the real transport and inject faults.
 */
public interface KurinTransport {

    /**
     * @return a future completed with the value of the response, or exceptionally with a
     * {@link org.kurin.api.exception.KurinException} (remote error, closed connection, timeout)
     */
    CompletableFuture<Object> sendRequest(NodeAddress destination, Object payload, Duration timeout);
}
