package org.kurin.kurinlocksspringbootstarter;

import org.kurin.kurinspringbootstarter.starter.KurinNodeBuilderCustomizer;
import org.kurin.raft.KurinNode;
import org.kurin.test.FaultInjectingTransport;
import org.kurin.test.NetworkFaults;
import org.kurin.test.TestPorts;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.stream.Collectors;

import static org.awaitility.Awaitility.await;

/**
 * Several Spring application contexts in one JVM that form a Kurin cluster on loopback, with a shared fault plan.
 */
final class SpringCluster implements AutoCloseable {
    private final List<ConfigurableApplicationContext> contexts = new ArrayList<>();
    private final NetworkFaults faults = new NetworkFaults();
    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor(task -> {
        Thread thread = new Thread(task, "spring-cluster-faults");
        thread.setDaemon(true);
        return thread;
    });

    SpringCluster(int nodes, Class<?> configuration, String... extraProperties) {
        List<Integer> ports = TestPorts.freePorts(nodes);
        String peers = ports.stream().map(port -> "127.0.0.1:" + port).collect(Collectors.joining(","));
        for (int port : ports) {
            String nodeId = "127.0.0.1:" + port;
            List<String> properties = new ArrayList<>(List.of(
                    "kurin.node.advertised-host=127.0.0.1",
                    "kurin.node.bind-host=127.0.0.1",
                    "kurin.node.port=" + port,
                    "kurin.node.peers=" + peers,
                    "kurin.node.storage=memory",
                    "kurin.node.heartbeat-interval=30ms",
                    "kurin.node.election-timeout-min=300ms",
                    "kurin.node.election-timeout-max=600ms",
                    "spring.main.banner-mode=off"));
            properties.addAll(List.of(extraProperties));

            contexts.add(new SpringApplicationBuilder(configuration)
                    .web(WebApplicationType.NONE)
                    .properties(properties.toArray(String[]::new))
                    .initializers(context -> context.getBeanFactory().registerSingleton("faultInjection",
                            (KurinNodeBuilderCustomizer) builder -> builder.transportDecorator(
                                    transport -> new FaultInjectingTransport(nodeId, transport, faults, scheduler))))
                    .run());
        }
    }

    List<ConfigurableApplicationContext> contexts() {
        return contexts;
    }

    KurinNode node(ConfigurableApplicationContext context) {
        return context.getBean(KurinNode.class);
    }

    Optional<ConfigurableApplicationContext> leader() {
        return contexts.stream().filter(context -> node(context).leadership().isPresent()).findFirst();
    }

    ConfigurableApplicationContext awaitLeader() {
        return await().atMost(Duration.ofSeconds(15)).until(this::establishedLeader, Optional::isPresent).orElseThrow();
    }

    private Optional<ConfigurableApplicationContext> establishedLeader() {
        Optional<ConfigurableApplicationContext> leader = leader();
        if (leader.isEmpty()) {
            return leader;
        }
        String leaderId = node(leader.get()).nodeId();
        List<KurinNode> reachable = contexts.stream().map(this::node)
                .filter(node -> !isCutOff(leaderId, node.nodeId()))
                .toList();
        boolean followed = reachable.stream().allMatch(node -> leaderId.equals(node.status().leaderId()));
        // A leader that was just cut off still holds its lease for a moment; it reaches no majority and does not count
        return followed && reachable.size() >= contexts.size() / 2 + 1 ? leader : Optional.empty();
    }

    private boolean isCutOff(String from, String to) {
        return cutOff.contains(from) != cutOff.contains(to);
    }

    private Set<String> cutOff = Set.of();

    /**
     * Separates the given node from all the others.
     */
    void isolate(ConfigurableApplicationContext isolated) {
        String isolatedId = node(isolated).nodeId();
        Set<String> rest = contexts.stream().map(context -> node(context).nodeId())
                .filter(id -> !id.equals(isolatedId)).collect(Collectors.toSet());
        cutOff = Set.of(isolatedId);
        faults.partition(List.of(Set.of(isolatedId), rest));
    }

    void heal() {
        cutOff = Set.of();
        faults.heal();
    }

    @Override
    public void close() {
        contexts.forEach(ConfigurableApplicationContext::close);
        scheduler.shutdownNow();
    }
}
