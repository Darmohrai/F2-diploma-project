package org.kurin.routing.shard;

import org.kurin.routing.KurinRoutingException;
import org.kurin.routing.KurinShardKey;

import java.lang.reflect.Field;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Maps an object, or a plain key, to the shard that owns it.
 */
public class KurinShardRouter {
    private final Set<String> shards;
    private final ShardResolver resolver;
    private final Map<Class<?>, Optional<Field>> shardKeyFields = new ConcurrentHashMap<>();

    public KurinShardRouter(Set<String> shards, ShardResolver resolver) {
        this.shards = Set.copyOf(shards);
        this.resolver = resolver;
    }

    public Set<String> shards() {
        return shards;
    }

    public ShardResolver resolver() {
        return resolver;
    }

    /**
     * @param target an object with a {@link KurinShardKey} field, or the shard key value itself
     * @return name of the shard that owns the key
     * @throws KurinRoutingException if the key is null or the resolver names an unknown shard
     */
    public String route(Object target) {
        Object shardKey = extractShardKey(target);
        if (shardKey == null) {
            throw new KurinRoutingException("Shard key is null"
                    + (target != null ? " in " + target.getClass().getSimpleName() : ""));
        }
        String shard = resolver.resolve(shardKey, shards);
        if (shard == null || !shards.contains(shard)) {
            throw new KurinRoutingException("Shard resolver " + resolver.getClass().getName() + " returned '" + shard
                    + "' for key '" + shardKey + "', which is not one of the configured shards " + shards);
        }
        return shard;
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
            throw new KurinRoutingException("Cannot read @KurinShardKey field " + field.get(), e);
        }
    }

    private static Optional<Field> findShardKeyField(Class<?> type) {
        for (Class<?> current = type; current != null && current != Object.class; current = current.getSuperclass()) {
            if (current.getName().startsWith("java.")) {
                break;
            }
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
