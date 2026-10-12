package org.kurin.api;

import java.time.Duration;

/**
 * @param requestId idempotency key: a command resubmitted with the same id is applied at most once.
 *                  Null means a fresh id is generated.
 * @param timeout   how long to wait for the result. Null means the node default.
 */
public record CommandOptions(String requestId, Duration timeout) {

    public static CommandOptions defaults() {
        return new CommandOptions(null, null);
    }

    public static CommandOptions withRequestId(String requestId) {
        return new CommandOptions(requestId, null);
    }

    public static CommandOptions withTimeout(Duration timeout) {
        return new CommandOptions(null, timeout);
    }

    public CommandOptions requestId(String requestId) {
        return new CommandOptions(requestId, timeout);
    }

    public CommandOptions timeout(Duration timeout) {
        return new CommandOptions(requestId, timeout);
    }
}
