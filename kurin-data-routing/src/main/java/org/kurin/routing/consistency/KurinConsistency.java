package org.kurin.routing.consistency;

import java.util.concurrent.Callable;

/**
 * The consistency token of the unit of work the current thread is running: what the client is known to have
 * written so far. In a web application a filter fills it from the request and writes it back to the response.
 * Elsewhere (message consumers, jobs, calls between services) the application passes the token itself:
 *
 * <pre>{@code
 * ConsistencyToken after = KurinConsistency.callWithToken(received, () -> {
 *     orders.place(order);          // a write: the token is updated when its transaction commits
 *     return KurinConsistency.current();
 * });
 * // hand `after` to whoever reads next
 * }</pre>
 */
public final class KurinConsistency {
    private static final ThreadLocal<ConsistencyToken> CURRENT = new ThreadLocal<>();

    private KurinConsistency() {
    }

    /**
     * @return the token of the current unit of work; empty if the caller has no known writes
     */
    public static ConsistencyToken current() {
        ConsistencyToken token = CURRENT.get();
        return token != null ? token : ConsistencyToken.empty();
    }

    public static void withToken(ConsistencyToken token, Runnable action) {
        ConsistencyToken previous = bind(token);
        try {
            action.run();
        } finally {
            restore(previous);
        }
    }

    public static <T> T callWithToken(ConsistencyToken token, Callable<T> action) throws Exception {
        ConsistencyToken previous = bind(token);
        try {
            return action.call();
        } finally {
            restore(previous);
        }
    }

    /**
     * Makes the token current for this thread.
     *
     * @return what was current before, to be passed to {@link #restore}
     */
    public static ConsistencyToken bind(ConsistencyToken token) {
        ConsistencyToken previous = CURRENT.get();
        CURRENT.set(token != null ? token : ConsistencyToken.empty());
        return previous;
    }

    public static void restore(ConsistencyToken previous) {
        if (previous == null) {
            CURRENT.remove();
        } else {
            CURRENT.set(previous);
        }
    }

    /**
     * Notes a committed write of the current unit of work. Called by the routing data source.
     */
    public static void recordWrite(String shard, String position) {
        if (position != null) {
            CURRENT.set(current().with(shard, position));
        }
    }
}
