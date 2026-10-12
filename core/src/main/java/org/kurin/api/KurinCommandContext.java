package org.kurin.api;

/**
 * Facts about the log entry being applied. Available only inside a {@link KurinCommand} handler.
 * <p>
 * Handlers must be deterministic: every node applies the same entry and must reach the same state.
 * They must not read the wall clock, call databases or remote services, or use randomness.
 * Use {@link #timestampMs()} as the time of the command.
 *
 * @param logIndex    index of the entry in the Raft log, strictly increasing
 * @param term        term in which the entry was created
 * @param timestampMs wall clock of the leader when the entry was appended, identical on every node
 * @param requestId   idempotency key of the command, may be null
 */
public record KurinCommandContext(long logIndex, long term, long timestampMs, String requestId) {
    private static final ThreadLocal<KurinCommandContext> CURRENT = new ThreadLocal<>();

    /**
     * @throws IllegalStateException outside of a command handler
     */
    public static KurinCommandContext current() {
        KurinCommandContext context = CURRENT.get();
        if (context == null) {
            throw new IllegalStateException("KurinCommandContext is available only while a @KurinCommand handler runs");
        }
        return context;
    }

    public static boolean isActive() {
        return CURRENT.get() != null;
    }

    /**
     * Binds the context to the current thread. Called by the Raft core around a handler invocation.
     *
     * @return the context that was bound before, to be passed to {@link #restore}
     */
    public static KurinCommandContext bind(KurinCommandContext context) {
        KurinCommandContext previous = CURRENT.get();
        CURRENT.set(context);
        return previous;
    }

    public static void restore(KurinCommandContext previous) {
        if (previous != null) {
            CURRENT.set(previous);
        } else {
            CURRENT.remove();
        }
    }
}
