package org.kurin.raft.rpc;

public record ClientCommandRequest(
        String requestId,
        Object command
) {
}
