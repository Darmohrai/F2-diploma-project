package org.kurin.raft.rpc;

import java.util.HashMap;

/**
 * @param headers trace context of the caller, sent along when the request is forwarded to the leader.
 *                It is not part of the log entry. May be null.
 */
public record ClientCommandRequest(
        String requestId,
        Object command,
        HashMap<String, String> headers
) implements RaftRpc {

    public ClientCommandRequest withHeaders(java.util.Map<String, String> newHeaders) {
        return new ClientCommandRequest(requestId, command, newHeaders.isEmpty() ? null : new HashMap<>(newHeaders));
    }
}
