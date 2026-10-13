package org.kurin.routing.shard;

import java.util.List;

/**
 * The difference between two shard layouts: the parts of the hash space whose owner changes. A planning tool
 * for resharding. It tells which keys have to be copied where; moving the data is up to the application.
 */
public final class RingDiff {

    /**
     * Hashes in {@code (fromExclusive, toInclusive]} change their owner. For the one range that wraps around
     * the end of the hash space, {@code fromExclusive} is greater than {@code toInclusive}.
     */
    public record Range(long fromExclusive, long toInclusive, String oldOwner, String newOwner) {
        public boolean contains(long hash) {
            return fromExclusive < toInclusive
                    ? hash > fromExclusive && hash <= toInclusive
                    : hash > fromExclusive || hash <= toInclusive;
        }
    }

    private final ConsistentHashRing before;
    private final ConsistentHashRing after;
    private final List<Range> ranges;

    RingDiff(ConsistentHashRing before, ConsistentHashRing after, List<Range> ranges) {
        this.before = before;
        this.after = after;
        this.ranges = List.copyOf(ranges);
    }

    public List<Range> ranges() {
        return ranges;
    }

    /**
     * @return true if the key is owned by a different node in the new layout
     */
    public boolean moves(String key) {
        return !before.nodeFor(key).equals(after.nodeFor(key));
    }

    public String oldOwner(String key) {
        return before.nodeFor(key);
    }

    public String newOwner(String key) {
        return after.nodeFor(key);
    }

    /**
     * @return the share of the hash space, between 0 and 1, that changes its owner
     */
    public double movedFraction() {
        double moved = 0;
        for (Range range : ranges) {
            double length = (double) range.toInclusive() - (double) range.fromExclusive();
            moved += length > 0 ? length : length + 0x1p64;
        }
        return moved / 0x1p64;
    }
}
