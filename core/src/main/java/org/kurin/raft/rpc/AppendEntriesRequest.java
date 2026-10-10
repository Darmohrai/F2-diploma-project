package org.kurin.raft.rpc;

import org.kurin.network.model.NodeAddress;
import org.kurin.raft.log.LogEntry;

import java.util.List;

/**
 * Request sent by the leader to replicate log entries or as a heartbeat.
 *
 * @param term         the leader's current term
 * @param leaderId     the network address identifying the leader, allowing followers to redirect clients
 * @param prevLogIndex the index of the log entry immediately preceding the new entries
 * @param prevLogTerm  the term of the log entry at {@code prevLogIndex}
 * @param entries      the new log entries to append; empty when this request is a heartbeat
 * @param leaderCommit the index of the highest log entry known to be committed by the leader
 */
public record AppendEntriesRequest(
        long term,
        NodeAddress leaderId,
        long prevLogIndex,
        long prevLogTerm,
        List<LogEntry> entries,
        long leaderCommit
) implements RaftRpc {
}
