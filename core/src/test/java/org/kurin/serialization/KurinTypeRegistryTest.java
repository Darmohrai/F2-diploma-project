package org.kurin.serialization;

import org.junit.jupiter.api.Test;
import org.kurin.api.KurinType;
import org.kurin.api.exception.KurinSerializationException;
import org.kurin.network.serializer.KryoSerializer;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class KurinTypeRegistryTest {

    public enum Status {NEW, PAID}

    public record Money(BigDecimal amount, String currency) {
    }

    public record Line(String sku, int quantity, Money price) {
    }

    public record Order(UUID id, Status status, List<Line> lines, Map<String, Money> totals, Instant createdAt, Line[] extras) {
    }

    public sealed interface Event permits Created, Cancelled {
    }

    public record Created(String id) implements Event {
    }

    public record Cancelled(String id, String reason) implements Event {
    }

    public record Envelope(Event event) {
    }

    @KurinType(id = 1500)
    public record Explicit(String value) {
    }

    @KurinType(id = 1500)
    public record ExplicitClash(String value) {
    }

    @KurinType(id = 7)
    public record TooLowId(String value) {
    }

    public record Unknown(String value) {
    }

    public static class Handlers {
        public Optional<Order> find(UUID id) {
            return Optional.empty();
        }
    }

    @Test
    void registersTypesTransitively() {
        KurinTypeRegistry registry = new KurinTypeRegistry().register(Order.class).register(Envelope.class);

        assertThat(registry.types().values())
                .contains(Order.class, Status.class, Line.class, Money.class, Line[].class, Envelope.class, Created.class, Cancelled.class)
                .doesNotContain(Event.class);
    }

    @Test
    void registersParameterAndResultTypesOfHandlers() throws Exception {
        KurinTypeRegistry registry = new KurinTypeRegistry()
                .registerHandler(Handlers.class.getMethod("find", UUID.class));

        assertThat(registry.types().values()).contains(Order.class, Money.class, Optional.class, UUID.class);
    }

    @Test
    void idsAndFingerprintDoNotDependOnRegistrationOrder() {
        KurinTypeRegistry forward = new KurinTypeRegistry().register(Order.class).register(Envelope.class).register(Explicit.class);
        KurinTypeRegistry backward = new KurinTypeRegistry().register(Explicit.class).register(Envelope.class).register(Order.class);

        assertThat(forward.types()).isEqualTo(backward.types());
        assertThat(forward.fingerprint()).isEqualTo(backward.fingerprint());
        assertThat(new KurinTypeRegistry().register(Order.class).fingerprint()).isNotEqualTo(forward.fingerprint());
    }

    @Test
    void usesExplicitIdsAndRejectsCollisions() {
        KurinTypeRegistry registry = new KurinTypeRegistry().register(Explicit.class);

        assertThat(registry.types()).containsEntry(1500, Explicit.class);
        assertThatThrownBy(() -> registry.register(ExplicitClash.class))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("1500")
                .hasMessageContaining("@KurinType");
        assertThatThrownBy(() -> new KurinTypeRegistry().register(TooLowId.class))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("at least 1000");
    }

    @Test
    void rejectsRegistrationAfterFreeze() {
        KurinTypeRegistry registry = new KurinTypeRegistry();
        registry.freeze();

        assertThatThrownBy(() -> registry.register(Order.class)).isInstanceOf(IllegalStateException.class);
    }

    @Test
    void serializesRegisteredGraphsWithJdkValueTypesAndImmutableCollections() {
        KryoSerializer serializer = KryoSerializer.standalone(Order.class, Envelope.class);
        Money price = new Money(new BigDecimal("19.90"), "EUR");
        Order order = new Order(UUID.randomUUID(), Status.PAID, List.of(new Line("A-1", 2, price)),
                Map.of("total", price), Instant.parse("2026-01-02T03:04:05Z"), new Line[]{new Line("B-2", 1, price)});

        Order copy = (Order) serializer.fromBytes(serializer.toBytes(order));
        assertThat(copy).usingRecursiveComparison().isEqualTo(order);

        assertThat(roundTrip(serializer, new Envelope(new Cancelled("7", "duplicate")))).isEqualTo(new Envelope(new Cancelled("7", "duplicate")));
        assertThat(roundTrip(serializer, List.of())).isEqualTo(List.of());
        assertThat(roundTrip(serializer, List.of(1, 2, 3))).isEqualTo(List.of(1, 2, 3));
        assertThat(roundTrip(serializer, Set.of("a"))).isEqualTo(Set.of("a"));
        assertThat(roundTrip(serializer, Map.of("k", 1L))).isEqualTo(Map.of("k", 1L));
        assertThat(roundTrip(serializer, Collections.emptyList())).isEqualTo(List.of());
        assertThat(roundTrip(serializer, Collections.unmodifiableList(new ArrayList<>(List.of("x", "y"))))).isEqualTo(List.of("x", "y"));
        assertThat(roundTrip(serializer, Collections.unmodifiableMap(Map.of("k", "v")))).isEqualTo(Map.of("k", "v"));
        assertThat(roundTrip(serializer, Optional.of("value"))).isEqualTo(Optional.of("value"));
        assertThat(roundTrip(serializer, Duration.ofSeconds(90))).isEqualTo(Duration.ofSeconds(90));
        assertThat(roundTrip(serializer, LocalDate.of(2026, 10, 12))).isEqualTo(LocalDate.of(2026, 10, 12));
        assertThat(roundTrip(serializer, (Object) null)).isNull();
    }

    @Test
    void unmodifiableCollectionsStayUnmodifiableAfterRoundTrip() {
        KryoSerializer serializer = KryoSerializer.standalone();

        @SuppressWarnings("unchecked")
        List<String> copy = (List<String>) roundTrip(serializer, Collections.unmodifiableList(new ArrayList<>(List.of("x"))));

        assertThatThrownBy(() -> copy.add("y")).isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void refusesUnregisteredTypes() {
        KryoSerializer serializer = KryoSerializer.standalone();

        assertThatThrownBy(() -> serializer.toBytes(new Unknown("x")))
                .isInstanceOf(KurinSerializationException.class)
                .hasMessageContaining(Unknown.class.getName());
    }

    private static Object roundTrip(KryoSerializer serializer, Object value) {
        return serializer.fromBytes(serializer.toBytes(value));
    }
}
