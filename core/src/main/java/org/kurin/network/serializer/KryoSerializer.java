package org.kurin.network.serializer;

import com.esotericsoftware.kryo.kryo5.Kryo;
import com.esotericsoftware.kryo.kryo5.KryoException;
import com.esotericsoftware.kryo.kryo5.io.Input;
import com.esotericsoftware.kryo.kryo5.io.Output;
import com.esotericsoftware.kryo.kryo5.objenesis.strategy.StdInstantiatorStrategy;
import com.esotericsoftware.kryo.kryo5.serializers.ImmutableCollectionsSerializers;
import com.esotericsoftware.kryo.kryo5.util.DefaultInstantiatorStrategy;
import com.esotericsoftware.kryo.kryo5.util.Pool;
import org.kurin.api.exception.KurinSerializationException;
import org.kurin.serialization.KurinTypeRegistry;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Map;

/**
 * The single serializer of a node: used for network messages, the Raft log and snapshots.
 * Only types of the frozen {@link KurinTypeRegistry} can be written or read.
 */
public class KryoSerializer {
    private final KurinTypeRegistry registry;
    private final Map<Integer, Class<?>> types;
    // A pool instead of ThreadLocal: instances are not pinned to (possibly virtual) threads
    private final Pool<Kryo> pool;

    public KryoSerializer(KurinTypeRegistry registry) {
        if (!registry.isFrozen()) {
            throw new IllegalStateException("KurinTypeRegistry must be frozen before a serializer is created");
        }
        this.registry = registry;
        this.types = registry.types();
        this.pool = new Pool<>(true, false, 64) {
            @Override
            protected Kryo create() {
                return newKryo();
            }
        };
    }

    /**
     * Creates a serializer with its own frozen registry holding the built-in types and the given ones.
     */
    public static KryoSerializer standalone(Class<?>... additionalTypes) {
        KurinTypeRegistry registry = new KurinTypeRegistry();
        for (Class<?> type : additionalTypes) {
            registry.register(type);
        }
        registry.freeze();
        return new KryoSerializer(registry);
    }

    public KurinTypeRegistry registry() {
        return registry;
    }

    private Kryo newKryo() {
        Kryo kryo = new Kryo();
        kryo.setRegistrationRequired(true);
        kryo.setInstantiatorStrategy(new DefaultInstantiatorStrategy(new StdInstantiatorStrategy()));
        ImmutableCollectionsSerializers.addDefaultSerializers(kryo);
        org.kurin.serialization.KryoRegistrations.apply(kryo, types);
        return kryo;
    }

    public void serialize(Object obj, OutputStream outStream) {
        Kryo kryo = pool.obtain();
        try {
            Output output = new Output(outStream);
            kryo.writeClassAndObject(output, obj);
            output.flush();
        } catch (KryoException | IllegalArgumentException e) {
            throw new KurinSerializationException("Cannot serialize "
                    + (obj != null ? obj.getClass().getName() : "null") + ": " + e.getMessage(), e);
        } finally {
            pool.free(kryo);
        }
    }

    public Object deserialize(InputStream inStream) {
        Kryo kryo = pool.obtain();
        try {
            return kryo.readClassAndObject(new Input(inStream));
        } catch (KryoException | IllegalArgumentException | IndexOutOfBoundsException e) {
            throw new KurinSerializationException("Cannot deserialize payload: " + e.getMessage(), e);
        } finally {
            pool.free(kryo);
        }
    }

    public byte[] toBytes(Object obj) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        serialize(obj, out);
        return out.toByteArray();
    }

    public Object fromBytes(byte[] bytes) {
        return fromBytes(bytes, 0, bytes.length);
    }

    public Object fromBytes(byte[] bytes, int offset, int length) {
        return deserialize(new ByteArrayInputStream(bytes, offset, length));
    }
}
