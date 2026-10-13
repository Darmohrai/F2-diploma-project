package org.kurin.routing.consistency;

import javax.sql.DataSource;
import java.time.Duration;

/**
 * The default strategy: the token remembers when the client last wrote, and reads go to the primary for a
 * fixed window after that. It needs nothing from the database, but it is a bet that replicas lag less than the
 * window; set the window above the replication lag you are prepared to see. Clocks of the application
 * instances should agree to well within the window.
 */
public class TimestampTokenStrategy implements ConsistencyTokenStrategy {
    private final long stickyWindowMs;

    public TimestampTokenStrategy(Duration stickyWindow) {
        this.stickyWindowMs = stickyWindow.toMillis();
    }

    @Override
    public String onWriteCommitted(String shard, DataSource primary) {
        return Long.toString(System.currentTimeMillis());
    }

    @Override
    public boolean requiresPrimary(String position, String shard, DataSource replica) {
        if (position == null) {
            return false;
        }
        try {
            return System.currentTimeMillis() - Long.parseLong(position) <= stickyWindowMs;
        } catch (NumberFormatException e) {
            // Not ours to interpret: stay on the safe side
            return true;
        }
    }
}
