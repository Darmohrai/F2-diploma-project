package org.kurin.raft.rpc;

public record ClientQueryRequest(
        String requestId,
        Object query
) implements RaftRpc {
}