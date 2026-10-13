package org.kurin.it;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.kurin.raft.KurinNode;
import org.kurin.routing.shard.KurinShardRouter;
import org.kurin.test.TestPorts;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.jdbc.core.JdbcTemplate;

import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Three instances of the application in one JVM, each with its own Kurin node, cache directory and web server,
 * all on the same four databases. The same sources run on Spring Boot 3 ({@code kurin-it-boot3}) and on
 * Spring Boot 4 ({@code kurin-it-boot4}); nothing here may depend on API that differs between them.
 */
class KurinApplicationTest {
    private static final Duration SOON = Duration.ofSeconds(20);
    private static final String TOKEN_HEADER = "X-Kurin-Consistency";

    @TempDir
    static Path directory;

    private static final String databaseId = "it-" + UUID.randomUUID();
    private static final List<ConfigurableApplicationContext> instances = new ArrayList<>();
    private static final HttpClient http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build();

    @BeforeAll
    static void startThreeInstances() {
        Shared.reset();
        // The primaries get their schema from the Liquibase change log of the application, on every shard.
        // Replicas would get it by replication; there is none here, so it is put there by hand.
        for (String replica : List.of("eu-replica", "us-replica")) {
            database(replica).execute("create table accounts (id varchar(64) primary key, name varchar(128) not null)");
        }
        List<Integer> ports = TestPorts.freePorts(3);
        String peers = ports.stream().map(port -> "127.0.0.1:" + port).collect(Collectors.joining(","));
        for (int i = 0; i < ports.size(); i++) {
            instances.add(new SpringApplicationBuilder(ItApplication.class)
                    .properties(
                            "server.port=0",
                            "spring.jmx.enabled=false",
                            "spring.main.banner-mode=off",
                            "it.database-id=" + databaseId,
                            "kurin.node.port=" + ports.get(i),
                            "kurin.node.peers=" + peers,
                            "kurin.cache.data-dir=" + directory.resolve("instance-" + i).resolve("cache"))
                    .run());
        }
    }

    @AfterAll
    static void stopInstances() {
        instances.forEach(ConfigurableApplicationContext::close);
        instances.clear();
    }

    // ---------------------------------------------------------------- helpers

    private static String url(int instance, String path) {
        return "http://127.0.0.1:" + instances.get(instance).getEnvironment().getProperty("local.server.port") + path;
    }

    private static HttpResponse<String> send(HttpRequest.Builder request, String... headers) throws Exception {
        request.timeout(Duration.ofSeconds(20));
        for (int i = 0; i < headers.length; i += 2) {
            request.header(headers[i], headers[i + 1]);
        }
        return http.send(request.build(), HttpResponse.BodyHandlers.ofString());
    }

    private static HttpResponse<String> get(int instance, String path, String... headers) throws Exception {
        return send(HttpRequest.newBuilder(URI.create(url(instance, path))).GET(), headers);
    }

    private static HttpResponse<String> post(int instance, String path, String... headers) throws Exception {
        return send(HttpRequest.newBuilder(URI.create(url(instance, path))).POST(HttpRequest.BodyPublishers.noBody()), headers);
    }

    private static String encode(String value) {
        return URLEncoder.encode(value, StandardCharsets.UTF_8);
    }

    private static KurinNode node(int instance) {
        return instances.get(instance).getBean(KurinNode.class);
    }

    private static void awaitLeader() {
        await("a leader known to all three instances").atMost(SOON).until(() -> {
            long leaders = instances.stream().filter(context -> context.getBean(KurinNode.class).leadership().isPresent()).count();
            String leaderId = node(0).status().leaderId();
            return leaders == 1 && leaderId != null
                    && instances.stream().allMatch(context -> leaderId.equals(context.getBean(KurinNode.class).status().leaderId()));
        });
    }

    private static JdbcTemplate database(String node) {
        return new JdbcTemplate(Databases.node(databaseId, node));
    }

    private static int totalProfileLoads() {
        return instances.stream().mapToInt(context -> context.getBean(Profiles.class).loads()).sum();
    }

    // ---------------------------------------------------------------- scenarios

    @Test
    void oneLeaderIsElectedAndCommandsFromAnyInstanceReachAll() throws Exception {
        awaitLeader();
        int before = Integer.parseInt(get(0, "/counter").body());
        assertThat(post(0, "/counter?delta=5").body()).isEqualTo(String.valueOf(before + 5));
        assertThat(post(1, "/counter?delta=7").body()).isEqualTo(String.valueOf(before + 12));
        for (int i = 0; i < instances.size(); i++) {
            assertThat(get(i, "/counter").body()).isEqualTo(String.valueOf(before + 12));
        }
    }

    @Test
    void scheduledOnceJobRunsOnOneInstancePerSlot() {
        awaitLeader();
        // Every instance fires its schedule for the same slot
        for (ConfigurableApplicationContext instance : instances) {
            instance.getBean(Coordination.class).nightly();
        }
        assertThat(Shared.JOB_RUNS).hasSize(1);
        // A second trigger in the same slot, on any instance, runs nothing
        for (ConfigurableApplicationContext instance : instances) {
            instance.getBean(Coordination.class).nightly();
        }
        assertThat(Shared.JOB_RUNS).hasSize(1);
    }

    @Test
    void distributedLockAdmitsOneInstanceAtATime() throws Exception {
        awaitLeader();
        int runsBefore = Shared.LOCKED_RUNS.get();
        ExecutorService threads = Executors.newFixedThreadPool(6);
        try {
            List<Future<?>> calls = new ArrayList<>();
            for (int i = 0; i < 12; i++) {
                Coordination coordination = instances.get(i % instances.size()).getBean(Coordination.class);
                calls.add(threads.submit(coordination::exclusive));
            }
            for (Future<?> call : calls) {
                call.get(60, TimeUnit.SECONDS);
            }
        } finally {
            threads.shutdownNow();
        }
        assertThat(Shared.LOCKED_RUNS.get() - runsBefore).isEqualTo(12);
        assertThat(Shared.MOST_INSIDE_LOCK.get()).isEqualTo(1);
    }

    @Test
    void cacheInvalidationReachesEveryInstanceAfterCommit() throws Exception {
        awaitLeader();
        assertThat(post(0, "/profiles/p1?name=" + encode("first name")).body()).isEqualTo("renamed");
        for (int i = 0; i < instances.size(); i++) {
            assertThat(get(i, "/profiles/p1").body()).isEqualTo("profile of first name");
        }
        // Cached now: reading again loads nothing, on any instance
        int loadsBefore = totalProfileLoads();
        for (int i = 0; i < instances.size(); i++) {
            assertThat(get(i, "/profiles/p1").body()).isEqualTo("profile of first name");
        }
        assertThat(totalProfileLoads()).isEqualTo(loadsBefore);

        // One instance changes the account in a transaction; the entry is dropped on all of them
        assertThat(post(1, "/profiles/p1?name=" + encode("second name")).body()).isEqualTo("renamed");
        await("every instance serves the new value").atMost(SOON).untilAsserted(() -> {
            for (int i = 0; i < instances.size(); i++) {
                assertThat(get(i, "/profiles/p1").body()).isEqualTo("profile of second name");
            }
        });
    }

    @Test
    void readYourWritesFollowsTheClientAcrossInstances() throws Exception {
        awaitLeader();
        // The replicas are not replicated: they keep what is put there by hand
        String shard = instances.get(0).getBean(KurinShardRouter.class).route("customer-1");
        database(shard + "-replica").update("insert into accounts (id, name) values (?, ?)", "customer-1", "stale");

        HttpResponse<String> write = post(0, "/accounts/customer-1?name=fresh");
        assertThat(write.body()).isEqualTo("saved");
        String token = write.headers().firstValue(TOKEN_HEADER).orElseThrow();
        assertThat(database(shard + "-primary").queryForObject("select name from accounts where id = ?", String.class, "customer-1"))
                .isEqualTo("fresh");

        // Another instance, same client: the token says it wrote, so it reads from the primary
        assertThat(get(1, "/accounts/customer-1", TOKEN_HEADER, token).body()).isEqualTo("fresh");
        // A client that did not write is served by the replica
        assertThat(get(2, "/accounts/customer-1").body()).isEqualTo("stale");
        // And so is anyone who asks for an eventual read
        assertThat(get(1, "/accounts/customer-1/stale", TOKEN_HEADER, token).body()).isEqualTo("stale");
    }

    @Test
    void healthIsUpAndTheEndpointsDescribeTheInstance() throws Exception {
        awaitLeader();
        await("health of every instance").atMost(SOON).untilAsserted(() -> {
            for (int i = 0; i < instances.size(); i++) {
                HttpResponse<String> health = get(i, "/actuator/health");
                // 503 unless the overall status is UP
                assertThat(health.statusCode()).as(health.body()).isEqualTo(200);
                // The overall status is the first field with Spring Boot 3 and the last with Spring Boot 4
                assertThat(health.body()).matches(body -> body.startsWith("{\"status\":\"UP\"") || body.endsWith("\"status\":\"UP\"}"),
                        "reports the overall status UP");
            }
        });
        String health = get(0, "/actuator/health").body();
        assertThat(health).contains("\"kurin\":", "\"kurinCache\":", "\"kurinRouting\":", "\"db\":",
                "\"eu.primary\":", "\"eu.replica\":", "\"us.primary\":", "\"us.replica\":");

        String kurin = get(0, "/actuator/kurin").body();
        assertThat(kurin).contains("\"node\":", "\"coordination\":", "\"caches\":", "\"routing\":",
                "\"nodeId\":\"" + node(0).nodeId() + "\"");
    }

    @Test
    void metersOfEveryModuleArePublished() throws Exception {
        awaitLeader();
        // Something of each kind has to happen before its meter exists
        post(0, "/counter?delta=1");
        post(0, "/profiles/m1?name=metrics");
        get(0, "/profiles/m1");
        instances.get(0).getBean(Coordination.class).exclusive();

        for (String meter : List.of("kurin.raft.term", "kurin.raft.storage.failed", "kurin.raft.apply.duration", "kurin.locks.acquire",
                "kurin.cache.gets", "kurin.routing.connections", "kurin.routing.node.up")) {
            HttpResponse<String> response = get(0, "/actuator/metrics/" + meter);
            assertThat(response.statusCode()).as(meter).isEqualTo(200);
            assertThat(response.body()).contains("\"name\":\"" + meter + "\"");
        }
    }
}
