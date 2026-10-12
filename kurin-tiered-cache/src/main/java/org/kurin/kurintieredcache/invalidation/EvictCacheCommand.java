package org.kurin.kurintieredcache.invalidation;

import java.util.List;

/**
 * Replicated request to drop cache entries on every instance.
 *
 * @param invalidationId unique id, by which the originating instance recognises its own request being applied
 * @param keys           the keys to remove; ignored when {@code allEntries} is set
 * @param originNodeId   the instance that issued the request and already cleaned the shared tier
 */
public record EvictCacheCommand(String invalidationId, String cacheName, List<String> keys, boolean allEntries,
                                String originNodeId) {
}
