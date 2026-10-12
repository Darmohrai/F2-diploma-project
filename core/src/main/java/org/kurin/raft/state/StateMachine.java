package org.kurin.raft.state;

/**
 * The replicated state. Every method is called on the Raft thread only.
 */
public interface StateMachine {

    /**
     * Applies a committed command. An exception is the result of the command, not a Raft failure:
     * throw {@link HandlerFailedException} to report the original exception of the application handler.
     */
    Object apply(Object command);

    Object executeQuery(Object query);

    byte[] takeSnapshot();

    void installSnapshot(byte[] data);

    void addListener(StateMachineListener listener);

    /**
     * Carries the exception thrown by an application handler out of the state machine.
     */
    class HandlerFailedException extends RuntimeException {
        public HandlerFailedException(Throwable cause) {
            super(cause);
        }
    }
}
