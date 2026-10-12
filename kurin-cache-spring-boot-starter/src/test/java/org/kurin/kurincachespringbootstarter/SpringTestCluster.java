package org.kurin.kurincachespringbootstarter;

import org.kurin.raft.KurinNode;
import org.kurin.test.TestPorts;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;

import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.awaitility.Awaitility.await;

/**
 * Several Spring application contexts in one JVM that form a Kurin cluster on loopback: the instances of one
 * application. Each gets its own cache directory under the given temporary directory.
 */
final class SpringTestCluster implements AutoCloseable {
    private final List<ConfigurableApplicationContext> contexts = new ArrayList<>();

    SpringTestCluster(int nodes, Class<?> configuration, Path directory, String... extraProperties) {
        List<Integer> ports = TestPorts.freePorts(nodes);
        String peers = ports.stream().map(port -> "127.0.0.1:" + port).collect(Collectors.joining(","));
        for (int index = 0; index < nodes; index++) {
            List<String> properties = new ArrayList<>(List.of(
                    "kurin.node.advertised-host=127.0.0.1",
                    "kurin.node.bind-host=127.0.0.1",
                    "kurin.node.port=" + ports.get(index),
                    "kurin.node.peers=" + peers,
                    "kurin.node.storage=memory",
                    "kurin.node.heartbeat-interval=30ms",
                    "kurin.node.election-timeout-min=300ms",
                    "kurin.node.election-timeout-max=600ms",
                    "kurin.cache.data-dir=" + directory.resolve("instance-" + index).toString().replace('\\', '/'),
                    "spring.main.banner-mode=off"));
            properties.addAll(List.of(extraProperties));
            contexts.add(new SpringApplicationBuilder(configuration)
                    .web(WebApplicationType.NONE)
                    .properties(properties.toArray(String[]::new))
                    .run());
        }
        awaitLeader();
    }

    List<ConfigurableApplicationContext> contexts() {
        return contexts;
    }

    <T> List<T> beans(Class<T> type) {
        return contexts.stream().map(context -> context.getBean(type)).toList();
    }

    private void awaitLeader() {
        await().atMost(Duration.ofSeconds(20)).until(() -> {
            Optional<KurinNode> leader = beans(KurinNode.class).stream().filter(node -> node.leadership().isPresent()).findFirst();
            return leader.isPresent() && beans(KurinNode.class).stream()
                    .allMatch(node -> leader.get().nodeId().equals(node.status().leaderId()));
        });
    }

    @Override
    public void close() {
        contexts.forEach(ConfigurableApplicationContext::close);
    }
}
