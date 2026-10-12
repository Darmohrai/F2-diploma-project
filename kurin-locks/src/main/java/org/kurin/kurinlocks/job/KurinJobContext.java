package org.kurin.kurinlocks.job;

/**
 * Facts about the run-once job the current thread is executing.
 *
 * @param slot         number of the time slot this run was granted
 * @param fencingToken token of the grant; pass it to external systems so that they can reject a run whose
 *                     slot was taken over in the meantime
 */
public record KurinJobContext(String jobName, long slot, long fencingToken) {
    private static final ThreadLocal<KurinJobContext> CURRENT = new ThreadLocal<>();

    /**
     * @throws IllegalStateException if the current thread is not running a granted job
     */
    public static KurinJobContext current() {
        KurinJobContext context = CURRENT.get();
        if (context == null) {
            throw new IllegalStateException("The current thread is not running a Kurin job");
        }
        return context;
    }

    public static boolean isActive() {
        return CURRENT.get() != null;
    }

    static KurinJobContext bind(KurinJobContext context) {
        KurinJobContext previous = CURRENT.get();
        CURRENT.set(context);
        return previous;
    }

    static void restore(KurinJobContext previous) {
        if (previous != null) {
            CURRENT.set(previous);
        } else {
            CURRENT.remove();
        }
    }
}
