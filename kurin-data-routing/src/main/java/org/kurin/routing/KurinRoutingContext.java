package org.kurin.routing;

/**
 * What the current thread asked of the router: which kind of node, how consistent, and which shard.
 * Each request is made for a scope and undone when the scope closes, so nested requests restore the outer one
 * and nothing leaks into the next use of a pooled thread.
 *
 * <pre>{@code
 * try (KurinRoutingContext.Scope ignored = KurinRoutingContext.readOnly(Consistency.EVENTUAL)) {
 *     return jdbc.query(...);
 * }
 * }</pre>
 */
public final class KurinRoutingContext {

    /**
     * @param type        PRIMARY if the primary was demanded, REPLICA if a replica is allowed, null if nothing was said
     * @param consistency how fresh a replica read must be; null unless {@code type} is REPLICA
     * @param shard       the selected shard, or null
     */
    private record State(DataSourceType type, KurinReadOnly.Consistency consistency, String shard) {
        static final State EMPTY = new State(null, null, null);
    }

    public interface Scope extends AutoCloseable {
        @Override
        void close();
    }

    private static final ThreadLocal<State> CURRENT = new ThreadLocal<>();

    private KurinRoutingContext() {
    }

    private static State state() {
        State state = CURRENT.get();
        return state != null ? state : State.EMPTY;
    }

    private static Scope enter(State next) {
        State previous = CURRENT.get();
        CURRENT.set(next);
        return () -> {
            if (previous == null) {
                CURRENT.remove();
            } else {
                CURRENT.set(previous);
            }
        };
    }

    /**
     * Allows replicas for the scope. Has no effect inside a scope that demands the primary.
     */
    public static Scope readOnly(KurinReadOnly.Consistency consistency) {
        State current = state();
        if (current.type() == DataSourceType.PRIMARY) {
            return enter(current);
        }
        return enter(new State(DataSourceType.REPLICA, consistency, current.shard()));
    }

    /**
     * Demands the primary for the scope.
     */
    public static Scope primary() {
        return enter(new State(DataSourceType.PRIMARY, null, state().shard()));
    }

    /**
     * Selects the shard for the scope.
     */
    public static Scope shard(String shard) {
        State current = state();
        return enter(new State(current.type(), current.consistency(), shard));
    }

    /**
     * @return PRIMARY, REPLICA, or null if the current thread expressed no preference
     */
    public static DataSourceType requestedType() {
        return state().type();
    }

    public static KurinReadOnly.Consistency consistency() {
        return state().consistency();
    }

    public static String shard() {
        return state().shard();
    }

    /**
     * Captures the context of the current thread so that work handed to another thread can run under it.
     */
    public static Runnable propagating(Runnable task) {
        State captured = CURRENT.get();
        return () -> {
            try (Scope ignored = enter(captured != null ? captured : State.EMPTY)) {
                task.run();
            }
        };
    }
}
