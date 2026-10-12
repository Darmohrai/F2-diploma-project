package org.kurin.kurinlocksspringbootstarter.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Runs the method under a cluster-wide lock, so that at most one instance of the application executes it for a
 * given key at a time.
 * <p>
 * The lock is a lease of {@link #ttl()}: it protects for that long, not for as long as the method runs. Choose a
 * ttl above the longest expected run or enable {@link #autoRenew()}. Inside the method
 * {@code KurinLockContext.current()} gives the handle with its fencing token.
 *
 * <pre>{@code
 * @KurinDistributedLock(key = "'invoice:' + #invoiceId", ttl = "PT30S", onBusy = OnBusy.THROW)
 * public void settle(String invoiceId) { ... }
 * }</pre>
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
public @interface KurinDistributedLock {

    enum OnBusy {
        /**
         * Do not run the method; return null (or an empty Optional). Not allowed for primitive return types.
         */
        SKIP,
        /**
         * Throw {@code KurinLockAcquisitionException}.
         */
        THROW,
        /**
         * Wait up to {@link #waitTimeout()} for the lock, then throw {@code KurinLockAcquisitionException}.
         * Requires a positive wait timeout.
         */
        WAIT
    }

    /**
     * SpEL expression of the lock key. Arguments are available as {@code #name}, {@code #a0}, {@code #p0};
     * a constant key must be quoted: {@code "'nightly-report'"}. An expression that fails or yields null
     * raises an exception; it is never used as a literal.
     */
    String key();

    /**
     * Time to live of the lock, as an ISO-8601 duration ({@code PT30S}) or in Spring style ({@code 30s}).
     */
    String ttl() default "PT30S";

    /**
     * How long to wait for a busy lock before {@link #onBusy()} applies. Zero means a single attempt.
     */
    String waitTimeout() default "PT0S";

    OnBusy onBusy() default OnBusy.SKIP;

    /**
     * Keep extending the lock every third of its ttl while the method runs.
     */
    boolean autoRenew() default false;

    /**
     * Let the thread that already holds the lock enter again without contacting the cluster.
     */
    boolean reentrant() default false;
}
