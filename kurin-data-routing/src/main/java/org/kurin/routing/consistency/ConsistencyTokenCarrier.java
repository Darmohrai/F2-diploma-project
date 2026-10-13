package org.kurin.routing.consistency;

import java.util.Optional;

/**
 * Moves the encoded consistency token between the client and the application: reads it from an incoming
 * request and writes it into the outgoing response. The server keeps no session for it. The servlet
 * implementation uses a header and, optionally, a cookie; other transports (messaging, gRPC) supply their own.
 *
 * @param <I> the incoming message
 * @param <O> the outgoing message
 */
public interface ConsistencyTokenCarrier<I, O> {

    /**
     * @return the encoded token found in the request, if any
     */
    Optional<String> read(I request);

    void write(O response, String encodedToken);
}
