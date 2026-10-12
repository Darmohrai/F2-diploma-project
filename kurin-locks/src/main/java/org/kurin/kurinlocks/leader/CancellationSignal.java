package org.kurin.kurinlocks.leader;

/**
 * Tells a running leader task that this node is no longer the leader and the task should stop.
 * The thread of the task is interrupted as well.
 */
public interface CancellationSignal {
    boolean isCancelled();
}
