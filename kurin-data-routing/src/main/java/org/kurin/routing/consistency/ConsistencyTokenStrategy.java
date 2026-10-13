package org.kurin.routing.consistency;

import javax.sql.DataSource;

/**
 * Decides when a client that wrote must read from the primary, and for how long.
 */
public interface ConsistencyTokenStrategy {

    /**
     * Called after a write transaction committed on the primary of a shard.
     *
     * @return the value to remember in the client's token for this shard, or null to remember nothing
     */
    String onWriteCommitted(String shard, DataSource primary);

    /**
     * @param position what {@link #onWriteCommitted} returned for the client's last write to the shard,
     *                 or null if the client's token records no write there
     * @return true if a replica read could miss that write, so the read must go to the primary
     */
    boolean requiresPrimary(String position, String shard, DataSource replica);
}
