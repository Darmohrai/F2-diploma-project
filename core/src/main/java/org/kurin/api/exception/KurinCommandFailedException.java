package org.kurin.api.exception;

/**
 * The command was committed and applied, and its handler threw an exception. This is the deterministic
 * result of the command: every node observed the same failure.
 */
public class KurinCommandFailedException extends KurinException {
    private final String causeType;
    private final String causeMessage;

    public KurinCommandFailedException(String causeType, String causeMessage) {
        super("Command handler failed with " + causeType + (causeMessage != null ? ": " + causeMessage : ""));
        this.causeType = causeType;
        this.causeMessage = causeMessage;
    }

    /**
     * @return fully qualified class name of the exception thrown by the handler
     */
    public String causeType() {
        return causeType;
    }

    public String causeMessage() {
        return causeMessage;
    }
}
