package org.kurin.serialization;

import com.esotericsoftware.kryo.kryo5.Kryo;
import com.esotericsoftware.kryo.kryo5.Serializer;
import com.esotericsoftware.kryo.kryo5.io.Input;
import com.esotericsoftware.kryo.kryo5.io.Output;
import org.kurin.network.dto.RpcResult;
import org.kurin.network.model.NodeAddress;
import org.kurin.raft.log.LogEntry;
import org.kurin.raft.log.LogEntryType;
import org.kurin.raft.rpc.AppendEntriesRequest;
import org.kurin.raft.rpc.AppendEntriesResponse;
import org.kurin.raft.rpc.ClientCommandRequest;
import org.kurin.raft.rpc.ClientQueryRequest;
import org.kurin.raft.rpc.RequestVoteRequest;
import org.kurin.raft.rpc.RequestVoteResponse;
import org.kurin.raft.state.CommandOutcome;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.UUID;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * Types with fixed wire ids below {@link KurinTypeRegistry#MIN_EXPLICIT_ID}: JDK value types, collections
 * and the messages of the framework itself. Ids listed here must never be reused for another type.
 */
final class BuiltinTypes {

    /**
     * @param serializer custom serializer, or null to use the Kryo default for the type
     */
    record Builtin(int id, Class<?> type, Supplier<Serializer<?>> serializer) {
    }

    private static final List<Builtin> ALL = build();

    private BuiltinTypes() {
    }

    static List<Builtin> all() {
        return ALL;
    }

    private static List<Builtin> build() {
        List<Builtin> types = new ArrayList<>();

        add(types, 10, ArrayList.class);
        add(types, 11, HashMap.class);
        add(types, 12, LinkedHashMap.class);
        add(types, 13, byte[].class);
        add(types, 14, HashSet.class);
        add(types, 15, LinkedHashSet.class);
        add(types, 16, TreeMap.class);
        add(types, 17, TreeSet.class);
        add(types, 18, LinkedList.class);
        add(types, 19, ArrayDeque.class);

        add(types, 20, NodeAddress.class);
        add(types, 21, LogEntry.class);
        add(types, 22, AppendEntriesRequest.class);
        add(types, 23, AppendEntriesResponse.class);
        add(types, 24, RequestVoteRequest.class);
        add(types, 25, RequestVoteResponse.class);
        add(types, 26, ClientCommandRequest.class);
        add(types, 27, org.kurin.raft.rpc.InstallSnapshotRequest.class);
        add(types, 28, org.kurin.raft.rpc.InstallSnapshotResponse.class);
        add(types, 29, ClientQueryRequest.class);
        add(types, 30, RpcResult.class);
        add(types, 31, LogEntryType.class);
        add(types, 32, CommandOutcome.class);
        add(types, 33, org.kurin.raft.rpc.PreVoteRequest.class);
        add(types, 34, org.kurin.raft.rpc.PreVoteResponse.class);
        add(types, 35, org.kurin.api.ReadConsistency.class);
        add(types, 36, org.kurin.raft.snapshot.Snapshot.class);

        add(types, 40, Object[].class);
        add(types, 41, String[].class);
        add(types, 42, int[].class);
        add(types, 43, long[].class);
        add(types, 44, double[].class);
        add(types, 45, boolean[].class);

        add(types, 50, Instant.class);
        add(types, 51, LocalDate.class);
        add(types, 52, LocalDateTime.class);
        add(types, 53, OffsetDateTime.class);
        add(types, 54, Duration.class);
        add(types, 55, LocalTime.class);

        types.add(new Builtin(60, UUID.class, com.esotericsoftware.kryo.kryo5.serializers.DefaultSerializers.UUIDSerializer::new));
        add(types, 61, BigDecimal.class);
        add(types, 62, BigInteger.class);
        add(types, 63, Optional.class);

        // Hidden implementation classes of List.of / Set.of / Map.of (serializers come from
        // ImmutableCollectionsSerializers, installed as Kryo defaults by KryoSerializer)
        add(types, 70, List.of().getClass());
        add(types, 71, List.of(1).getClass());
        add(types, 72, Set.of().getClass());
        add(types, 73, Set.of(1).getClass());
        add(types, 74, Map.of().getClass());
        add(types, 75, Map.of(1, 1).getClass());

        add(types, 80, Collections.emptyList().getClass());
        add(types, 81, Collections.emptySet().getClass());
        add(types, 82, Collections.emptyMap().getClass());
        add(types, 83, Collections.singletonList(1).getClass());
        add(types, 84, Collections.singleton(1).getClass());
        add(types, 85, Collections.singletonMap(1, 1).getClass());

        addUnmodifiable(types, 90, Collections.unmodifiableCollection(new ArrayList<>()),
                value -> new ArrayList<>((Collection<?>) value), copy -> Collections.unmodifiableCollection((Collection<?>) copy));
        addUnmodifiable(types, 91, Collections.unmodifiableList(new ArrayList<>()),
                value -> new ArrayList<>((Collection<?>) value), copy -> Collections.unmodifiableList((List<?>) copy));
        addUnmodifiable(types, 92, Collections.unmodifiableList(new LinkedList<>()),
                value -> new ArrayList<>((Collection<?>) value), copy -> Collections.unmodifiableList(new LinkedList<>((List<?>) copy)));
        addUnmodifiable(types, 93, Collections.unmodifiableSet(new HashSet<>()),
                value -> new LinkedHashSet<>((Collection<?>) value), copy -> Collections.unmodifiableSet((Set<?>) copy));
        addUnmodifiable(types, 94, Collections.unmodifiableMap(new HashMap<>()),
                value -> new LinkedHashMap<>((Map<?, ?>) value), copy -> Collections.unmodifiableMap((Map<?, ?>) copy));
        addUnmodifiable(types, 95, Collections.unmodifiableSortedSet(new TreeSet<>()),
                value -> new TreeSet<>((SortedSet<?>) value), copy -> Collections.unmodifiableSortedSet((SortedSet<?>) copy));
        addUnmodifiable(types, 96, Collections.unmodifiableSortedMap(new TreeMap<>()),
                value -> new TreeMap<>((SortedMap<?, ?>) value), copy -> Collections.unmodifiableSortedMap((SortedMap<?, ?>) copy));
        addUnmodifiable(types, 97, Collections.unmodifiableNavigableSet(new TreeSet<>()),
                value -> new TreeSet<>((SortedSet<?>) value), copy -> Collections.unmodifiableNavigableSet((TreeSet<?>) copy));
        addUnmodifiable(types, 98, Collections.unmodifiableNavigableMap(new TreeMap<>()),
                value -> new TreeMap<>((SortedMap<?, ?>) value), copy -> Collections.unmodifiableNavigableMap((TreeMap<?, ?>) copy));

        return List.copyOf(types);
    }

    private static void add(List<Builtin> types, int id, Class<?> type) {
        types.add(new Builtin(id, type, null));
    }

    private static void addUnmodifiable(List<Builtin> types, int id, Object sample,
                                        Function<Object, Object> toMutable, Function<Object, Object> wrap) {
        types.add(new Builtin(id, sample.getClass(), () -> new UnmodifiableWrapperSerializer(toMutable, wrap)));
    }

    /**
     * Writes a {@code Collections.unmodifiable*} view as a plain mutable copy and wraps it again on read.
     */
    private static final class UnmodifiableWrapperSerializer extends Serializer<Object> {
        private final Function<Object, Object> toMutable;
        private final Function<Object, Object> wrap;

        UnmodifiableWrapperSerializer(Function<Object, Object> toMutable, Function<Object, Object> wrap) {
            super(false, true);
            this.toMutable = toMutable;
            this.wrap = wrap;
        }

        @Override
        public void write(Kryo kryo, Output output, Object value) {
            kryo.writeClassAndObject(output, toMutable.apply(value));
        }

        @Override
        public Object read(Kryo kryo, Input input, Class<?> type) {
            return wrap.apply(kryo.readClassAndObject(input));
        }
    }
}
