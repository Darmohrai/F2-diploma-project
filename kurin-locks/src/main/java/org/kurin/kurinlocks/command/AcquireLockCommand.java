package org.kurin.kurinlocks.command;

public record AcquireLockCommand(
        String lockKey,
        String ownerId,
        long ttlMs) {
}
