package org.kurin.routing.consistency;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Replication positions of PostgreSQL streaming replication, as the number of WAL bytes since the start:
 * {@code pg_current_wal_lsn()} on the primary and {@code pg_last_wal_replay_lsn()} on the replica.
 */
public class PostgresReplicaPositionProbe implements ReplicaPositionProbe {
    private static final String PRIMARY_POSITION = "SELECT pg_wal_lsn_diff(pg_current_wal_lsn(), '0/0')::bigint";
    private static final String REPLICA_POSITION = "SELECT pg_wal_lsn_diff(pg_last_wal_replay_lsn(), '0/0')::bigint";

    @Override
    public long currentPrimaryPosition(String shard, DataSource primary) throws SQLException {
        return queryLong(primary, PRIMARY_POSITION);
    }

    @Override
    public long replicaReplayedPosition(String shard, DataSource replica) throws SQLException {
        return queryLong(replica, REPLICA_POSITION);
    }

    private static long queryLong(DataSource dataSource, String sql) throws SQLException {
        try (Connection connection = dataSource.getConnection();
             Statement statement = connection.createStatement();
             ResultSet result = statement.executeQuery(sql)) {
            if (!result.next()) {
                throw new SQLException("No row returned by: " + sql);
            }
            long position = result.getLong(1);
            if (result.wasNull()) {
                // pg_last_wal_replay_lsn() is null on a server that is not in recovery, that is, not a replica
                throw new SQLException("The server reports no replication position; is it a replica? Query: " + sql);
            }
            return position;
        }
    }
}
