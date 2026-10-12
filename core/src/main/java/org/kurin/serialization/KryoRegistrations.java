package org.kurin.serialization;

import com.esotericsoftware.kryo.kryo5.Kryo;
import com.esotericsoftware.kryo.kryo5.Serializer;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;

/**
 * Applies the content of a {@link KurinTypeRegistry} to a Kryo instance.
 */
public final class KryoRegistrations {
    private static final Map<Class<?>, Supplier<Serializer<?>>> CUSTOM_SERIALIZERS = new HashMap<>();

    static {
        for (BuiltinTypes.Builtin builtin : BuiltinTypes.all()) {
            if (builtin.serializer() != null) {
                CUSTOM_SERIALIZERS.put(builtin.type(), builtin.serializer());
            }
        }
    }

    private KryoRegistrations() {
    }

    /**
     * Teaches a Kryo instance to handle the JDK types that its built-in defaults cannot: immutable collections
     * ({@code List.of} and friends), {@code Collections.unmodifiable*} views and {@code UUID}. For serializers
     * that do not use a {@link KurinTypeRegistry}.
     */
    public static void addJdkDefaultSerializers(Kryo kryo) {
        com.esotericsoftware.kryo.kryo5.serializers.ImmutableCollectionsSerializers.addDefaultSerializers(kryo);
        CUSTOM_SERIALIZERS.forEach((type, serializer) -> kryo.addDefaultSerializer(type, serializer.get()));
    }

    public static void apply(Kryo kryo, Map<Integer, Class<?>> types) {
        types.forEach((id, type) -> {
            Supplier<Serializer<?>> custom = CUSTOM_SERIALIZERS.get(type);
            if (custom != null) {
                kryo.register(type, custom.get(), id);
            } else {
                kryo.register(type, id);
            }
        });
    }
}
