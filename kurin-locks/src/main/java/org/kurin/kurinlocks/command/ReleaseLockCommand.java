package org.kurin.kurinlocks.command;

public record ReleaseLockCommand(
        String lockKey,
        String ownerId) {
}
