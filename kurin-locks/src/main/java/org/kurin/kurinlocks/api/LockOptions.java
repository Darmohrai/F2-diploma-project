package org.kurin.kurinlocks.api;

import java.time.Duration;

/**
 * @param ttl         time to live of the lock; it is released automatically after that unless renewed
 * @param waitTimeout how long to wait for a busy lock; zero means a single attempt
 * @param autoRenew   keep extending the lock every {@code ttl / 3} until it is released
 * @param reentrant   let the thread that holds the lock acquire it again without contacting the cluster
 */
public record LockOptions(Duration ttl, Duration waitTimeout, boolean autoRenew, boolean reentrant) {

    public LockOptions {
        if (ttl == null || ttl.isZero() || ttl.isNegative()) {
            throw new IllegalArgumentException("Lock ttl must be positive");
        }
        if (waitTimeout == null || waitTimeout.isNegative()) {
            throw new IllegalArgumentException("Lock waitTimeout must not be negative");
        }
    }

    public static LockOptions ttl(Duration ttl) {
        return new LockOptions(ttl, Duration.ZERO, false, false);
    }

    public LockOptions waitTimeout(Duration waitTimeout) {
        return new LockOptions(ttl, waitTimeout, autoRenew, reentrant);
    }

    public LockOptions autoRenew(boolean autoRenew) {
        return new LockOptions(ttl, waitTimeout, autoRenew, reentrant);
    }

    public LockOptions reentrant(boolean reentrant) {
        return new LockOptions(ttl, waitTimeout, autoRenew, reentrant);
    }
}
