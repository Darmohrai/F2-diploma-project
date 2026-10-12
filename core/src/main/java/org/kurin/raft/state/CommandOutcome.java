package org.kurin.raft.state;

import org.kurin.api.exception.KurinCommandFailedException;

import java.util.concurrent.CompletableFuture;

/**
 * Result of applying a command: the value returned by the handler or the exception it threw. Both are
 * deterministic results, remembered for deduplication and identical on every node.
 *
 * @param errorType class name of the exception thrown by the handler, null on success
 */
public record CommandOutcome(boolean success, Object value, String errorType, String errorMessage) {

    public static CommandOutcome success(Object value) {
        return new CommandOutcome(true, value, null, null);
    }

    public static CommandOutcome failure(String errorType, String errorMessage) {
        return new CommandOutcome(false, null, errorType, errorMessage);
    }

    public void completeInto(CompletableFuture<Object> future) {
        if (success) {
            future.complete(value);
        } else {
            future.completeExceptionally(new KurinCommandFailedException(errorType, errorMessage));
        }
    }

    public CompletableFuture<Object> toFuture() {
        CompletableFuture<Object> future = new CompletableFuture<>();
        completeInto(future);
        return future;
    }
}
