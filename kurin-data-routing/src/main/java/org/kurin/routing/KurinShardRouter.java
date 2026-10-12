package org.kurin.routing;

import org.kurin.routing.KurinShardKey;

import java.lang.reflect.Field;
import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

public class KurinShardRouter {
    private final ConsistentHashRing ring;
    private final Map<Class<?>, Optional<Field>> shardKeyFields = new ConcurrentHashMap<>();

    public KurinShardRouter(Collection<String> shards, int virtualNodes) {
        this.ring = new ConsistentHashRing(shards, virtualNodes);
    }

    /**
     * @param target an object with a {@link KurinShardKey} field, or the shard key value itself
     * @return name of the shard that owns the key
     */
    public String route(Object target) {
        Object shardKey = extractShardKey(target);
        if (shardKey == null) {
            throw new IllegalArgumentException("Shard key is null" +
                    (target != null ? " in " + target.getClass().getSimpleName() : ""));
        }
        return ring.nodeFor(String.valueOf(shardKey));
    }

    private Object extractShardKey(Object target) {
        if (target == null) {
            return null;
        }
        Optional<Field> field = shardKeyFields.computeIfAbsent(target.getClass(), KurinShardRouter::findShardKeyField);
        if (field.isEmpty()) {
            return target;
        }
        try {
            return field.get().get(target);
        } catch (IllegalAccessException e) {
            throw new IllegalStateException("Cannot read @KurinShardKey field " + field.get(), e);
        }
    }

    private static Optional<Field> findShardKeyField(Class<?> type) {
        for (Class<?> current = type; current != null && current != Object.class; current = current.getSuperclass()) {
            for (Field field : current.getDeclaredFields()) {
                if (field.isAnnotationPresent(KurinShardKey.class)) {
                    field.setAccessible(true);
                    return Optional.of(field);
                }
            }
        }
        return Optional.empty();
    }
}
