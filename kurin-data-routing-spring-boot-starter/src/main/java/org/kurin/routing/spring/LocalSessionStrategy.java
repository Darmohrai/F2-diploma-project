package org.kurin.routing.spring;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import org.kurin.routing.consistency.ConsistencyTokenStrategy;

import javax.sql.DataSource;
import java.time.Duration;

/**
 * Remembers recent writers in the memory of this instance, by client id, and sends their reads to the primary
 * for the sticky window. No token travels with the client, so the guarantee holds only while the client keeps
 * talking to the instance that handled its write: use it for a single instance or behind sticky load balancing.
 */
public class LocalSessionStrategy implements ConsistencyTokenStrategy {
    private final KurinClientIdResolver clientIdResolver;
    // Entries expire on their own, so clients that never come back do not accumulate
    private final Cache<String, Boolean> recentWriters;

    public LocalSessionStrategy(KurinClientIdResolver clientIdResolver, Duration stickyWindow) {
        this.clientIdResolver = clientIdResolver;
        this.recentWriters = Caffeine.newBuilder()
                .expireAfterWrite(stickyWindow)
                .maximumSize(100_000)
                .build();
    }

    @Override
    public String onWriteCommitted(String shard, DataSource primary) {
        String clientId = clientIdResolver.resolveClientId();
        if (clientId != null) {
            recentWriters.put(key(clientId, shard), Boolean.TRUE);
        }
        return null;
    }

    @Override
    public boolean requiresPrimary(String position, String shard, DataSource replica) {
        String clientId = clientIdResolver.resolveClientId();
        return clientId != null && recentWriters.getIfPresent(key(clientId, shard)) != null;
    }

    private static String key(String clientId, String shard) {
        return shard + '\u0000' + clientId;
    }
}
