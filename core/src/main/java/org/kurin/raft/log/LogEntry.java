package org.kurin.raft.log;

/**
 * @param requestId         idempotency key of the client request, null for entries without one
 * @param leaderTimestampMs wall clock of the leader when it appended the entry; the only notion of time
 *                          a state machine may use
 */
public record LogEntry(
        long index,
        long term,
        LogEntryType type,
        String requestId,
        long leaderTimestampMs,
        Object command
) {
}
