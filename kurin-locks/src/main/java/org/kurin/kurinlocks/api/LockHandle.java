package org.kurin.kurinlocks.api;

import java.time.Duration;
import java.time.Instant;

/**
 * A lock held by this process. Close or {@link #release()} it when the protected work is done.
 * <p>
 * A lock is a lease: it ends at {@link #expiresAt()} whether or not the holder is finished. Work that must not
 * overlap under any circumstances should pass {@link #fencingToken()} to the resource it modifies and let the
 * resource reject tokens older than the newest it has seen.
 */
public interface LockHandle extends AutoCloseable {

    String key();

    /**
     * @return a number that is larger for every later grant of any lock in the cluster
     */
    long fencingToken();

    /**
     * @return when the lock expires unless renewed, in the clock of the cluster
     */
    Instant expiresAt();

    /**
     * Extends the lock to {@code ttl} from now.
     *
     * @return false if the lock was already lost (expired or taken over); the handle is then no longer held
     */
    boolean renew(Duration ttl);

    /**
     * Gives the lock up. Safe to call more than once. For a reentrant acquisition the lock itself is released
     * only when the outermost handle is.
     */
    void release();

    /**
     * @return true while this process may assume it holds the lock: not released, not reported lost, and its
     * time to live, measured conservatively on the local clock, has not passed
     */
    boolean isHeld();

    @Override
    default void close() {
        release();
    }
}
