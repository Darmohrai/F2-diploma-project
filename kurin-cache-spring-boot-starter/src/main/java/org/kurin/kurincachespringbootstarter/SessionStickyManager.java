package org.kurin.kurincachespringbootstarter;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;

import java.time.Duration;

public class SessionStickyManager {
    // Entries expire on their own, so clients that never come back do not accumulate
    private final Cache<String, Boolean> recentWriters;

    public SessionStickyManager(long stickyWindowMs) {
        this.recentWriters = Caffeine.newBuilder()
                .expireAfterWrite(Duration.ofMillis(stickyWindowMs))
                .maximumSize(100_000)
                .build();
    }

    public void markWriteForClient(String clientId) {
        if (clientId != null) {
            recentWriters.put(clientId, Boolean.TRUE);
        }
    }

    public boolean shouldRouteToPrimary(String clientId) {
        return clientId != null && recentWriters.getIfPresent(clientId) != null;
    }
}
