package org.kurin.kurinlocks.command;

/**
 * Commands, queries and results of the replicated lock state machine.
 */
public final class LockCommands {
    private LockCommands() {
    }

    /**
     * @param ownerId unique per acquisition attempt, so a command whose outcome is unknown can be undone safely
     */
    public record AcquireLock(String key, String ownerId, long ttlMs) {
    }

    public record RenewLock(String key, String ownerId, long ttlMs) {
    }

    public record ReleaseLock(String key, String ownerId) {
    }

    /**
     * Removes every lock whose time to live has passed.
     */
    public record PurgeExpiredLocks() {
    }

    /**
     * @param fencingToken log index of the command that granted the lock: larger for every later grant of any lock
     * @param expiresAtMs  expiry in the time of the state machine (leader timestamps of the log)
     */
    public record AcquireResult(boolean granted, long fencingToken, long expiresAtMs) {
        public static AcquireResult denied() {
            return new AcquireResult(false, 0, 0);
        }
    }

    public record RenewResult(boolean renewed, long expiresAtMs) {
    }

    public record LockRecord(String ownerId, long fencingToken, long expiresAtMs, long acquiredAtMs) {
    }

    /**
     * Query: the record of one lock, or null if the key is not locked. An expired record is still returned
     * until something removes it.
     */
    public record GetLock(String key) {
    }

    /**
     * Query: the number of lock records, expired ones included.
     */
    public record CountLocks() {
    }
}
