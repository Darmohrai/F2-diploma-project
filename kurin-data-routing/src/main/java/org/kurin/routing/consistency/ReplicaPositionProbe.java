package org.kurin.routing.consistency;

import javax.sql.DataSource;
import java.sql.SQLException;

/**
 * Reads replication positions from a database, as numbers that only grow: a replica has everything the
 * primary had at position {@code p} once its replayed position is at least {@code p}.
 */
public interface ReplicaPositionProbe {

    /**
     * @return the current write position of the primary
     */
    long currentPrimaryPosition(String shard, DataSource primary) throws SQLException;

    /**
     * @return the position up to which the replica has applied changes
     */
    long replicaReplayedPosition(String shard, DataSource replica) throws SQLException;
}
