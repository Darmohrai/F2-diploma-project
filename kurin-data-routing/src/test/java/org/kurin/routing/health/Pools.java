package org.kurin.routing.health;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import java.io.IOException;
import java.net.ServerSocket;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.Duration;
import org.awaitility.Awaitility;
import java.util.UUID;

/**
 * Real connection pools and addresses for the tests of this package.
 */
final class Pools {
    private Pools() {
    }

    /**
     * @return the URL of an empty in-memory database that lives until the JVM ends
     */
    static String memoryDatabase() {
        return "jdbc:h2:mem:" + UUID.randomUUID() + ";DB_CLOSE_DELAY=-1";
    }

    /**
     * @return a port on which nothing listens
     */
    static int closedPort() {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        } catch (IOException e) {
            throw new IllegalStateException("No free port", e);
        }
    }

    /**
     * @return the URL of a PostgreSQL server that does not exist
     */
    static String absentPostgres() {
        return "jdbc:postgresql://127.0.0.1:" + closedPort() + "/app";
    }

    /**
     * A HikariCP pool that starts without connecting and hands out at most {@code size} connections. A caller
     * waits for one as briefly as HikariCP allows: for pools that are meant to have none to give.
     */
    static HikariDataSource hikari(String url, int size) {
        return new HikariDataSource(hikariConfig(url, size));
    }

    /**
     * Waits until the pool has opened a connection, which on a cold JVM takes longer than the pool lets a caller
     * wait. After this a caller gets a connection at once or, if it is in use, the timeout of the pool.
     */
    static void awaitFirstConnection(HikariDataSource pool) {
        Awaitility.await("the first connection of " + pool.getPoolName()).atMost(Duration.ofSeconds(60))
                .until(() -> pool.getHikariPoolMXBean().getIdleConnections() > 0);
    }

    /**
     * Takes a connection from a pool that has had the time to open it.
     */
    static Connection take(HikariDataSource pool) throws SQLException {
        awaitFirstConnection(pool);
        return pool.getConnection();
    }

    static HikariConfig hikariConfig(String url, int size) {
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(url);
        config.setUsername("app");
        config.setPassword("secret");
        config.setMaximumPoolSize(size);
        // The shortest timeout HikariCP accepts
        config.setConnectionTimeout(250);
        // Do not fail in the constructor when the database is away
        config.setInitializationFailTimeout(-1);
        return config;
    }
}
