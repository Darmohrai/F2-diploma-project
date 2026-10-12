package org.kurin.kurintieredcache.codec;

import com.esotericsoftware.kryo.kryo5.Kryo;
import com.esotericsoftware.kryo.kryo5.KryoException;
import com.esotericsoftware.kryo.kryo5.Registration;
import com.esotericsoftware.kryo.kryo5.io.Input;
import com.esotericsoftware.kryo.kryo5.io.Output;
import com.esotericsoftware.kryo.kryo5.objenesis.strategy.StdInstantiatorStrategy;
import com.esotericsoftware.kryo.kryo5.util.DefaultClassResolver;
import com.esotericsoftware.kryo.kryo5.util.DefaultInstantiatorStrategy;
import com.esotericsoftware.kryo.kryo5.util.MapReferenceResolver;
import com.esotericsoftware.kryo.kryo5.util.Pool;
import org.kurin.kurintieredcache.api.KurinCacheException;
import org.kurin.kurintieredcache.api.KurinCacheValueCodec;
import org.kurin.kurintieredcache.core.CacheEntry;
import org.kurin.kurintieredcache.core.KurinNullValue;
import org.kurin.serialization.KryoRegistrations;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Set;

/**
 * Default codec of cache values.
 * <p>
 * Layout: {@code magic(2) version(1) flags(1) createdAt(8) [expiresAt(8)] [staleUntil(8)] [payload]}.
 * The payload is written by a Kryo instance of its own that needs no type registration, because cached values
 * are arbitrary data objects of the application.
 * <p>
 * Without registration the bytes name the classes to instantiate, so bytes from a store that others can write
 * to must not be trusted blindly: with a list of allowed packages the codec refuses every class outside of
 * them and outside a fixed set of JDK value and collection types.
 */
public final class KryoCacheCodec implements KurinCacheValueCodec {
    private static final byte MAGIC_0 = 'K';
    private static final byte MAGIC_1 = 'C';
    private static final byte VERSION = 1;
    private static final int FLAG_NULL = 1;
    private static final int FLAG_HAS_TTL = 2;
    private static final int FLAG_HAS_STALE = 4;
    private static final int FIXED_HEADER_BYTES = 12;

    private static final Set<String> ALLOWED_JAVA_LANG = Set.of(
            "java.lang.Object", "java.lang.String", "java.lang.Boolean", "java.lang.Byte", "java.lang.Short",
            "java.lang.Integer", "java.lang.Long", "java.lang.Float", "java.lang.Double", "java.lang.Character",
            "java.lang.Number", "java.lang.Enum", "java.lang.StringBuilder");
    private static final List<String> ALLOWED_JDK_PREFIXES = List.of("java.util.", "java.time.", "java.math.");

    private final List<String> allowedPackagePrefixes;
    private final Pool<Kryo> pool;

    /**
     * @param allowedPackages packages (with their subpackages) whose classes may appear in cached values.
     *                        Empty allows every class, which is acceptable only for stores nobody else writes to.
     */
    public KryoCacheCodec(List<String> allowedPackages) {
        this.allowedPackagePrefixes = allowedPackages.stream()
                .map(String::trim)
                .filter(name -> !name.isEmpty())
                .map(name -> name.endsWith(".") ? name : name + ".")
                .toList();
        this.pool = new Pool<>(true, false, 64) {
            @Override
            protected Kryo create() {
                return newKryo();
            }
        };
    }

    public boolean restrictsTypes() {
        return !allowedPackagePrefixes.isEmpty();
    }

    private Kryo newKryo() {
        Kryo kryo = new Kryo(new AllowListClassResolver(), new MapReferenceResolver());
        kryo.setRegistrationRequired(false);
        kryo.setInstantiatorStrategy(new DefaultInstantiatorStrategy(new StdInstantiatorStrategy()));
        KryoRegistrations.addJdkDefaultSerializers(kryo);
        return kryo;
    }

    private final class AllowListClassResolver extends DefaultClassResolver {
        // Every class met for the first time, on write and on read, passes through here
        @Override
        public Registration registerImplicit(Class type) {
            if (!isAllowed(type)) {
                throw new KryoException("Class " + type.getName() + " is not allowed in cached values. "
                        + "Add its package to kurin.cache.codec.allowed-packages if it is a data class of the application.");
            }
            return super.registerImplicit(type);
        }
    }

    private boolean isAllowed(Class<?> type) {
        if (allowedPackagePrefixes.isEmpty()) {
            return true;
        }
        Class<?> element = type;
        while (element.isArray()) {
            element = element.getComponentType();
        }
        if (element.isPrimitive() || element == KurinNullValue.class) {
            return true;
        }
        String name = element.getName();
        if (ALLOWED_JAVA_LANG.contains(name)) {
            return true;
        }
        for (String prefix : ALLOWED_JDK_PREFIXES) {
            if (name.startsWith(prefix)) {
                return true;
            }
        }
        for (String prefix : allowedPackagePrefixes) {
            if (name.startsWith(prefix)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public byte[] encode(CacheEntry entry) {
        int flags = (entry.isNullMarker() ? FLAG_NULL : 0)
                | (entry.expiresAtMs() != 0 ? FLAG_HAS_TTL : 0)
                | (entry.staleUntilMs() != 0 ? FLAG_HAS_STALE : 0);

        ByteArrayOutputStream bytes = new ByteArrayOutputStream(64);
        ByteBuffer header = ByteBuffer.allocate(FIXED_HEADER_BYTES + 16);
        header.put(MAGIC_0).put(MAGIC_1).put(VERSION).put((byte) flags).putLong(entry.createdAtMs());
        if (entry.expiresAtMs() != 0) {
            header.putLong(entry.expiresAtMs());
        }
        if (entry.staleUntilMs() != 0) {
            header.putLong(entry.staleUntilMs());
        }
        bytes.write(header.array(), 0, header.position());

        if (!entry.isNullMarker()) {
            Kryo kryo = pool.obtain();
            try {
                Output output = new Output(bytes);
                kryo.writeClassAndObject(output, entry.payload());
                output.flush();
            } catch (RuntimeException e) {
                throw new KurinCacheException("Cannot encode a cached value of type " + entry.payload().getClass().getName()
                        + ": " + e.getMessage(), e);
            } finally {
                kryo.reset();
                pool.free(kryo);
            }
        }
        return bytes.toByteArray();
    }

    private record Header(int flags, long createdAtMs, long expiresAtMs, long staleUntilMs, int payloadOffset) {
    }

    private static Header readHeader(byte[] bytes) {
        if (bytes == null || bytes.length < FIXED_HEADER_BYTES || bytes[0] != MAGIC_0 || bytes[1] != MAGIC_1) {
            throw new KurinCacheException("Not a Kurin cache entry: unknown header");
        }
        if (bytes[2] != VERSION) {
            throw new KurinCacheException("Unsupported cache entry format version " + bytes[2]);
        }
        try {
            ByteBuffer buffer = ByteBuffer.wrap(bytes, 3, bytes.length - 3);
            int flags = buffer.get();
            long createdAtMs = buffer.getLong();
            long expiresAtMs = (flags & FLAG_HAS_TTL) != 0 ? buffer.getLong() : 0;
            long staleUntilMs = (flags & FLAG_HAS_STALE) != 0 ? buffer.getLong() : 0;
            return new Header(flags, createdAtMs, expiresAtMs, staleUntilMs, buffer.position());
        } catch (RuntimeException e) {
            throw new KurinCacheException("Truncated cache entry header", e);
        }
    }

    @Override
    public CacheEntry decode(byte[] bytes) {
        Header header = readHeader(bytes);
        Object payload;
        if ((header.flags() & FLAG_NULL) != 0) {
            payload = KurinNullValue.INSTANCE;
        } else {
            Kryo kryo = pool.obtain();
            try {
                payload = kryo.readClassAndObject(new Input(bytes, header.payloadOffset(), bytes.length - header.payloadOffset()));
            } catch (RuntimeException e) {
                throw new KurinCacheException("Cannot decode a cached value: " + e.getMessage(), e);
            } finally {
                kryo.reset();
                pool.free(kryo);
            }
            if (payload == null) {
                throw new KurinCacheException("Cache entry holds no payload");
            }
        }
        return new CacheEntry(payload, header.createdAtMs(), header.expiresAtMs(), header.staleUntilMs());
    }

    @Override
    public long deadAtMs(byte[] bytes) {
        Header header = readHeader(bytes);
        return Math.max(header.expiresAtMs(), header.staleUntilMs());
    }
}
