package org.kurin.routing.health;

import com.zaxxer.hikari.HikariDataSource;
import org.junit.jupiter.api.Test;
import org.kurin.routing.DataSourceType;

import java.sql.Connection;
import java.sql.SQLException;
import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowableOfType;

/**
 * 1.1.2: the two ways in which load used to be taken for an outage. Both use a real pool of one connection that
 * the application holds, and only what the monitor offered in 1.1.1 already, so the same test can be run against
 * that version.
 */
class ExhaustedPoolIsNotADeadNodeTest {

    @Test
    void requestThatTimedOutWaitingForThePoolDoesNotMarkTheNodeDown() throws Exception {
        try (HikariDataSource pool = Pools.hikari(Pools.memoryDatabase(), 1);
             DataSourceHealthMonitor monitor = new DataSourceHealthMonitor(Duration.ofHours(1), Duration.ofSeconds(1), 2);
             Connection held = Pools.take(pool)) {
            monitor.register("A", DataSourceType.REPLICA, pool);

            // What the router reports when a request got no connection from the pool
            SQLException timeout = catchThrowableOfType(pool::getConnection, SQLException.class);
            monitor.reportFailure("A", DataSourceType.REPLICA, timeout);

            assertThat(held.isValid(1)).isTrue();
            assertThat(monitor.isUp("A", DataSourceType.REPLICA))
                    .as("a pool without a free connection belongs to a node that works").isTrue();
        }
    }

    @Test
    void periodicCheckDoesNotMarkTheNodeDownWhileTheApplicationHoldsEveryConnection() throws Exception {
        try (HikariDataSource pool = Pools.hikari(Pools.memoryDatabase(), 1);
             DataSourceHealthMonitor monitor = new DataSourceHealthMonitor(Duration.ofHours(1), Duration.ofSeconds(1), 2);
             Connection held = Pools.take(pool)) {
            monitor.register("A", DataSourceType.REPLICA, pool);

            // More checks than any threshold
            for (int check = 0; check < 4; check++) {
                monitor.checkAll();
            }

            assertThat(held.isValid(1)).isTrue();
            assertThat(monitor.isUp("A", DataSourceType.REPLICA))
                    .as("the check must not depend on a free connection in the pool of the application").isTrue();
        }
    }
}
