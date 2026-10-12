package org.kurin.kurinspringbootstarter.template;

import org.kurin.api.CommandOptions;
import org.kurin.api.LeadershipToken;
import org.kurin.api.ReadConsistency;
import org.kurin.raft.KurinNode;
import org.kurin.raft.RaftStatus;

import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

/**
 * Entry point of the application to the cluster: replicated commands, queries and leadership.
 * All futures fail with subclasses of {@link org.kurin.api.exception.KurinException}.
 */
public class KurinTemplate {
    private final KurinNode kurinNode;

    public KurinTemplate(KurinNode kurinNode) {
        this.kurinNode = kurinNode;
    }

    /**
     * Replicates the command and applies it on every node; completes with the value returned by its handler.
     */
    public <T> CompletableFuture<T> submitCommand(Object command) {
        return kurinNode.submitCommand(command);
    }

    public <T> CompletableFuture<T> submitCommand(Object command, Duration timeout) {
        return kurinNode.submitCommand(command, timeout);
    }

    /**
     * @param options carries the request id that makes a retry after a timeout idempotent
     */
    public <T> CompletableFuture<T> submitCommand(Object command, CommandOptions options) {
        return kurinNode.submitCommand(command, options);
    }

    /**
     * Runs a query with linearizable consistency: it observes every command acknowledged before the call.
     */
    public <T> CompletableFuture<T> query(Object query) {
        return kurinNode.submitQuery(query);
    }

    public <T> CompletableFuture<T> query(Object query, ReadConsistency consistency) {
        return kurinNode.submitQuery(query, consistency);
    }

    /**
     * @return true if this node currently believes it is the leader. Use {@link #leadership()} for decisions
     * that must hold on one node only.
     */
    public boolean isLeader() {
        return kurinNode.isLeader();
    }

    public RaftStatus status() {
        return kurinNode.status();
    }

    /**
     * @return a token while this node is provably the leader; its term is a fencing token for external systems
     */
    public Optional<LeadershipToken> leadership() {
        return kurinNode.leadership();
    }
}
