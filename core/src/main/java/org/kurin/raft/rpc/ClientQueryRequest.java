package org.kurin.raft.rpc;

import org.kurin.api.ReadConsistency;

import java.util.HashMap;

/**
 * @param headers trace context of the caller, sent along when the request is forwarded to the leader. May be null.
 */
public record ClientQueryRequest(
        String requestId,
        Object query,
        ReadConsistency consistency,
        HashMap<String, String> headers
) implements RaftRpc {

    public ClientQueryRequest withHeaders(java.util.Map<String, String> newHeaders) {
        return new ClientQueryRequest(requestId, query, consistency, newHeaders.isEmpty() ? null : new HashMap<>(newHeaders));
    }
}
