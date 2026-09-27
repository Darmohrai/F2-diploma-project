package org.kurin.raft.log;

public record LogEntry(
        long index,
        long term,
        String requestId,
        Object command
) {
}
