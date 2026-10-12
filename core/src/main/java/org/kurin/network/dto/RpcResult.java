package org.kurin.network.dto;

import org.kurin.api.exception.KurinCommandFailedException;
import org.kurin.api.exception.KurinException;
import org.kurin.api.exception.KurinNoLeaderException;
import org.kurin.api.exception.KurinNotLeaderException;
import org.kurin.api.exception.KurinNotStartedException;
import org.kurin.api.exception.KurinRemoteException;
import org.kurin.api.exception.KurinSerializationException;
import org.kurin.api.exception.KurinTimeoutException;
import org.kurin.network.model.NodeAddress;

import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;

/**
 * Envelope of every RESPONSE: a request is always answered, with a value (possibly null) or with an error.
 *
 * @param value        the result; for a failed command, the class name of the exception thrown by the handler
 * @param errorType    one of the {@code ERROR_*} codes, null on success
 * @param leaderHint   leader known to the responding node, set for {@link #ERROR_NOT_LEADER}
 */
public record RpcResult(boolean ok, Object value, String errorType, String errorMessage, NodeAddress leaderHint) {
    public static final String ERROR_NOT_LEADER = "NOT_LEADER";
    public static final String ERROR_NO_LEADER = "NO_LEADER";
    public static final String ERROR_TIMEOUT = "TIMEOUT";
    public static final String ERROR_COMMAND_FAILED = "COMMAND_FAILED";
    public static final String ERROR_SERIALIZATION = "SERIALIZATION";
    public static final String ERROR_NOT_STARTED = "NOT_STARTED";
    public static final String ERROR_REMOTE = "REMOTE";

    public static RpcResult success(Object value) {
        return new RpcResult(true, value, null, null, null);
    }

    public static RpcResult failure(Throwable error) {
        Throwable cause = unwrap(error);
        if (cause instanceof KurinNotLeaderException notLeader) {
            NodeAddress hint = notLeader.leaderHint() != null ? NodeAddress.parse(notLeader.leaderHint()) : null;
            return new RpcResult(false, null, ERROR_NOT_LEADER, cause.getMessage(), hint);
        }
        if (cause instanceof KurinCommandFailedException failed) {
            return new RpcResult(false, failed.causeType(), ERROR_COMMAND_FAILED, failed.causeMessage(), null);
        }
        if (cause instanceof KurinNoLeaderException) {
            return error(ERROR_NO_LEADER, cause.getMessage());
        }
        if (cause instanceof KurinTimeoutException) {
            return error(ERROR_TIMEOUT, cause.getMessage());
        }
        if (cause instanceof KurinSerializationException) {
            return error(ERROR_SERIALIZATION, cause.getMessage());
        }
        if (cause instanceof KurinNotStartedException) {
            return error(ERROR_NOT_STARTED, cause.getMessage());
        }
        if (cause instanceof KurinException) {
            return error(ERROR_REMOTE, cause.getMessage());
        }
        return error(ERROR_REMOTE, cause.getClass().getName() + ": " + cause.getMessage());
    }

    private static RpcResult error(String type, String message) {
        return new RpcResult(false, null, type, message, null);
    }

    /**
     * @return the exception to raise on the calling node for a failed result
     */
    public KurinException toException() {
        return switch (errorType) {
            case ERROR_NOT_LEADER -> new KurinNotLeaderException(errorMessage, leaderHint != null ? leaderHint.asString() : null);
            case ERROR_NO_LEADER -> new KurinNoLeaderException(errorMessage);
            case ERROR_TIMEOUT -> new KurinTimeoutException(errorMessage);
            case ERROR_COMMAND_FAILED -> new KurinCommandFailedException(String.valueOf(value), errorMessage);
            case ERROR_SERIALIZATION -> new KurinSerializationException(errorMessage);
            case ERROR_NOT_STARTED -> new KurinNotStartedException(errorMessage);
            default -> new KurinRemoteException(errorMessage);
        };
    }

    public static Throwable unwrap(Throwable error) {
        Throwable current = error;
        while ((current instanceof CompletionException || current instanceof ExecutionException) && current.getCause() != null) {
            current = current.getCause();
        }
        return current;
    }
}
