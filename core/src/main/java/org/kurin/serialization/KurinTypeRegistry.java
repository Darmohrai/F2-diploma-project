package org.kurin.serialization;

import org.kurin.api.KurinType;

import java.lang.reflect.Field;
import java.lang.reflect.GenericArrayType;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.RecordComponent;
import java.lang.reflect.Type;
import java.lang.reflect.TypeVariable;
import java.lang.reflect.WildcardType;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.HexFormat;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

/**
 * Assigns a stable wire id to every type that crosses the network or is written to the Raft log.
 * <p>
 * Ids do not depend on registration order: a type gets the id from {@link KurinType}, or an id derived
 * from its fully qualified name. Nodes compare {@link #fingerprint()} during the handshake, so two nodes
 * with different sets of types refuse to talk instead of silently decoding one class as another.
 * <p>
 * Registration is transitive: record components, fields, array components, generic arguments and permitted
 * subclasses of sealed types are registered too. Types reachable only through {@code Object}, a non-sealed
 * interface or an abstract class cannot be discovered and must be registered explicitly.
 */
public final class KurinTypeRegistry {
    public static final int MIN_EXPLICIT_ID = 1000;
    public static final int HASH_ID_BASE = 10_000;

    private final Map<Class<?>, Integer> idsByClass = new HashMap<>();
    private final Map<Integer, Class<?>> classesById = new HashMap<>();
    private final Set<Class<?>> visited = new HashSet<>();
    private boolean frozen;

    public KurinTypeRegistry() {
        for (BuiltinTypes.Builtin builtin : BuiltinTypes.all()) {
            assign(builtin.type(), builtin.id());
        }
    }

    /**
     * Registers the type and everything reachable from it.
     *
     * @throws IllegalStateException if the registry is frozen or two types resolve to the same id
     */
    public synchronized KurinTypeRegistry register(Type type) {
        ensureNotFrozen();
        if (type instanceof Class<?> clazz && isRegistrable(clazz) && isConcrete(clazz) && !idsByClass.containsKey(clazz)) {
            // An explicitly named type is registered even when the transitive walk would skip it (JDK classes)
            assign(clazz, resolveId(clazz));
        }
        walk(type);
        return this;
    }

    /**
     * Registers the parameter types and the result type of a command or query handler.
     */
    public synchronized KurinTypeRegistry registerHandler(Method handler) {
        ensureNotFrozen();
        for (Type parameterType : handler.getGenericParameterTypes()) {
            walk(parameterType);
        }
        walk(handler.getGenericReturnType());
        return this;
    }

    public synchronized void freeze() {
        frozen = true;
    }

    public synchronized boolean isFrozen() {
        return frozen;
    }

    /**
     * @return registered types by id, in ascending id order
     */
    public synchronized Map<Integer, Class<?>> types() {
        return Collections.unmodifiableMap(new TreeMap<>(classesById));
    }

    /**
     * @return SHA-256 over the sorted {@code id=className} list, hex encoded
     */
    public synchronized String fingerprint() {
        MessageDigest digest = sha256();
        for (Map.Entry<Integer, Class<?>> entry : new TreeMap<>(classesById).entrySet()) {
            digest.update((entry.getKey() + "=" + entry.getValue().getName() + "\n").getBytes(StandardCharsets.UTF_8));
        }
        return HexFormat.of().formatHex(digest.digest());
    }

    private void ensureNotFrozen() {
        if (frozen) {
            throw new IllegalStateException("KurinTypeRegistry is frozen: types must be registered before the node starts");
        }
    }

    private void walk(Type type) {
        if (type instanceof Class<?> clazz) {
            walkClass(clazz);
        } else if (type instanceof ParameterizedType parameterized) {
            walk(parameterized.getRawType());
            for (Type argument : parameterized.getActualTypeArguments()) {
                walk(argument);
            }
        } else if (type instanceof GenericArrayType array) {
            walk(array.getGenericComponentType());
        } else if (type instanceof WildcardType wildcard) {
            for (Type bound : wildcard.getUpperBounds()) {
                walk(bound);
            }
        } else if (type instanceof TypeVariable<?> variable) {
            for (Type bound : variable.getBounds()) {
                walk(bound);
            }
        }
    }

    private void walkClass(Class<?> clazz) {
        if (!visited.add(clazz) || !isRegistrable(clazz)) {
            return;
        }
        if (clazz.isArray()) {
            walkClass(clazz.getComponentType());
            assignIfAbsent(clazz);
            return;
        }
        if (clazz.isEnum()) {
            assignIfAbsent(clazz);
            return;
        }
        if (clazz.isSealed()) {
            for (Class<?> permitted : clazz.getPermittedSubclasses()) {
                walkClass(permitted);
            }
        }
        if (clazz.isInterface() || Modifier.isAbstract(clazz.getModifiers())) {
            return;
        }
        if (isJdkClass(clazz)) {
            // Internals of JDK classes are not walked; unsupported ones must be registered explicitly
            return;
        }

        assignIfAbsent(clazz);

        if (clazz.isRecord()) {
            for (RecordComponent component : clazz.getRecordComponents()) {
                walk(component.getGenericType());
            }
            return;
        }
        for (Class<?> current = clazz; current != null && current != Object.class; current = current.getSuperclass()) {
            for (Field field : current.getDeclaredFields()) {
                int modifiers = field.getModifiers();
                if (!Modifier.isStatic(modifiers) && !Modifier.isTransient(modifiers)) {
                    walk(field.getGenericType());
                }
            }
        }
    }

    private void assignIfAbsent(Class<?> clazz) {
        if (!idsByClass.containsKey(clazz)) {
            assign(clazz, resolveId(clazz));
        }
    }

    private void assign(Class<?> clazz, int id) {
        Class<?> existing = classesById.get(id);
        if (existing != null && existing != clazz) {
            throw new IllegalStateException("Wire id " + id + " is claimed by both " + existing.getName() + " and "
                    + clazz.getName() + ". Give one of them a unique id with @KurinType(id = ...).");
        }
        classesById.put(id, clazz);
        idsByClass.put(clazz, id);
    }

    private static int resolveId(Class<?> clazz) {
        KurinType explicit = clazz.getAnnotation(KurinType.class);
        if (explicit != null) {
            if (explicit.id() < MIN_EXPLICIT_ID) {
                throw new IllegalStateException("@KurinType id of " + clazz.getName() + " must be at least "
                        + MIN_EXPLICIT_ID + ", got " + explicit.id());
            }
            return explicit.id();
        }
        byte[] hash = sha256().digest(clazz.getName().getBytes(StandardCharsets.UTF_8));
        int value = ((hash[0] & 0xFF) << 24 | (hash[1] & 0xFF) << 16 | (hash[2] & 0xFF) << 8 | (hash[3] & 0xFF)) & Integer.MAX_VALUE;
        return HASH_ID_BASE + value % (Integer.MAX_VALUE - HASH_ID_BASE);
    }

    // Primitives, their wrappers and String are always known to Kryo; Object carries no type information
    private static boolean isRegistrable(Class<?> clazz) {
        return !clazz.isPrimitive()
                && clazz != Object.class && clazz != String.class && clazz != Void.class
                && clazz != Boolean.class && clazz != Byte.class && clazz != Character.class && clazz != Short.class
                && clazz != Integer.class && clazz != Long.class && clazz != Float.class && clazz != Double.class;
    }

    private static boolean isConcrete(Class<?> clazz) {
        return clazz.isArray() || clazz.isEnum() || (!clazz.isInterface() && !Modifier.isAbstract(clazz.getModifiers()));
    }

    private static boolean isJdkClass(Class<?> clazz) {
        String name = clazz.getName();
        return name.startsWith("java.") || name.startsWith("javax.") || name.startsWith("jdk.") || name.startsWith("sun.");
    }

    private static MessageDigest sha256() {
        try {
            return MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
    }
}
