package org.kurin.raft.log;

public enum LogEntryType {
    /**
     * A command of the application, passed to the state machine.
     */
    USER,
    /**
     * Written by a new leader to commit entries of earlier terms. Never reaches the state machine.
     */
    NOOP,
    /**
     * Reserved for entries consumed by the Raft core itself.
     */
    INTERNAL
}
