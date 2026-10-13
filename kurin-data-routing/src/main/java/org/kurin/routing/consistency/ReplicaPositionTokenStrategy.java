package org.kurin.routing.consistency;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.SQLException;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Exact read-your-writes: the token remembers the replication position of the primary right after the client's
 * write, and the client reads from a replica only once that replica has replayed the position. No guess about
 * lag is involved; the price is a query to the primary per write and a (cached) query to the replica per read.
 */
public class ReplicaPositionTokenStrategy implements ConsistencyTokenStrategy {
    private static final Logger log = LoggerFactory.getLogger(ReplicaPositionTokenStrategy.class);

    private record Sample(long position, long takenAtNanos) {
    }

    private final ReplicaPositionProbe probe;
    private final long cacheNanos;
    private final Map<String, Sample> replicaPositions = new ConcurrentHashMap<>();

    /**
     * @param probeCache how long a replica position read from the database is reused. A cached position can only
     *                   be too old, which sends a read to the primary that could have gone to the replica.
     */
    public ReplicaPositionTokenStrategy(ReplicaPositionProbe probe, Duration probeCache) {
        this.probe = probe;
        this.cacheNanos = probeCache.toNanos();
    }

    @Override
    public String onWriteCommitted(String shard, DataSource primary) {
        try {
            return Long.toString(probe.currentPrimaryPosition(shard, primary));
        } catch (SQLException | RuntimeException e) {
            log.warn("Cannot read the write position of the primary of shard '{}': {}. "
                    + "The client keeps reading from the primary until its next successful write.", shard, e.toString());
            // A position no replica can reach
            return Long.toString(Long.MAX_VALUE);
        }
    }

    @Override
    public boolean requiresPrimary(String position, String shard, DataSource replica) {
        if (position == null) {
            return false;
        }
        long required;
        try {
            required = Long.parseLong(position);
        } catch (NumberFormatException e) {
            return true;
        }
        Sample cached = replicaPositions.get(shard);
        if (cached != null && cached.position() >= required) {
            // Positions only grow: once reached, always reached
            return false;
        }
        if (cached != null && System.nanoTime() - cached.takenAtNanos() < cacheNanos) {
            return true;
        }
        try {
            long replayed = probe.replicaReplayedPosition(shard, replica);
            replicaPositions.put(shard, new Sample(replayed, System.nanoTime()));
            return replayed < required;
        } catch (SQLException | RuntimeException e) {
            log.debug("Cannot read the replayed position of the replica of shard '{}': {}", shard, e.toString());
            return true;
        }
    }

    /**
     * @return the replayed position of the replica of the shard when it was last probed; -1 if it was not probed yet
     */
    public long lastKnownReplicaPosition(String shard) {
        Sample sample = replicaPositions.get(shard);
        return sample != null ? sample.position() : -1;
    }
}
