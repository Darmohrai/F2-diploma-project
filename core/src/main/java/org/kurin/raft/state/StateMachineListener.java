package org.kurin.raft.state;

/**
 * Notified on every node after a command was applied successfully. Runs on the Raft thread:
 * it must be fast and must not block. An exception thrown here is logged and otherwise ignored.
 */
public interface StateMachineListener {
    void onCommandApplied(Object command, Object result);
}
