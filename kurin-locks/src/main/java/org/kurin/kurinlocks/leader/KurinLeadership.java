package org.kurin.kurinlocks.leader;

import org.kurin.api.LeadershipToken;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Optional;
import java.util.concurrent.Callable;

/**
 * The leadership under which the current thread is running a leader-only method or a leader task.
 */
public final class KurinLeadership {
    private static final ThreadLocal<Deque<LeadershipToken>> TOKENS = ThreadLocal.withInitial(ArrayDeque::new);

    private KurinLeadership() {
    }

    /**
     * @return the token checked when the current leader-only call started. It proves leadership at that moment;
     * pass its term to external systems so that they can reject work of a deposed leader.
     */
    public static Optional<LeadershipToken> currentToken() {
        return Optional.ofNullable(TOKENS.get().peek());
    }

    /**
     * Runs the action with the token bound to the current thread.
     */
    public static <T> T callWith(LeadershipToken token, Callable<T> action) throws Exception {
        Deque<LeadershipToken> tokens = TOKENS.get();
        tokens.push(token);
        try {
            return action.call();
        } finally {
            tokens.pop();
            if (tokens.isEmpty()) {
                TOKENS.remove();
            }
        }
    }
}
