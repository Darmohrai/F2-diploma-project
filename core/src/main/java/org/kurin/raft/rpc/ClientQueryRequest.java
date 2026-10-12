package org.kurin.raft.rpc;

import org.kurin.api.ReadConsistency;

public record ClientQueryRequest(
        String requestId,
        Object query,
        ReadConsistency consistency
) implements RaftRpc {
}
