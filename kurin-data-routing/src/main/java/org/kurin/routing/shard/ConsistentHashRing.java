package org.kurin.routing.shard;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.TreeSet;

/**
 * Consistent hash ring: every node owns the arcs that end at its virtual points. A node of weight {@code w}
 * gets {@code w} times as many points, and so about {@code w} times as many keys.
 */
public class ConsistentHashRing {
    private final NavigableMap<Long, String> ring = new TreeMap<>();
    private final Map<String, Integer> weights;

    /**
     * Ring of equally weighted nodes.
     */
    public ConsistentHashRing(Collection<String> nodes, int virtualNodes) {
        this(equalWeights(nodes), virtualNodes);
    }

    /**
     * @param weights      positive weight per node
     * @param virtualNodes points on the ring for a node of weight 1
     */
    public ConsistentHashRing(Map<String, Integer> weights, int virtualNodes) {
        if (weights == null || weights.isEmpty()) {
            throw new IllegalArgumentException("Hash ring must have at least one node");
        }
        if (virtualNodes <= 0) {
            throw new IllegalArgumentException("virtualNodes must be positive");
        }
        this.weights = Map.copyOf(weights);
        weights.forEach((node, weight) -> {
            if (weight <= 0) {
                throw new IllegalArgumentException("Weight of " + node + " must be positive");
            }
            // The points of a node depend only on its name, so its arcs stay put when other nodes come or go
            for (int i = 0; i < virtualNodes * weight; i++) {
                ring.put(hash(node + "#" + i), node);
            }
        });
    }

    private static Map<String, Integer> equalWeights(Collection<String> nodes) {
        Map<String, Integer> weights = new LinkedHashMap<>();
        if (nodes != null) {
            nodes.forEach(node -> weights.put(node, 1));
        }
        return weights;
    }

    public String nodeFor(String key) {
        return ownerOf(hash(key));
    }

    String ownerOf(long point) {
        Map.Entry<Long, String> entry = ring.ceilingEntry(point);
        return (entry != null ? entry : ring.firstEntry()).getValue();
    }

    public Map<String, Integer> weights() {
        return weights;
    }

    /**
     * Compares this ring with another one, typically the planned next layout of the shards.
     *
     * @return which parts of the hash space, and so which keys, change their owner
     */
    public RingDiff diff(ConsistentHashRing other) {
        TreeSet<Long> points = new TreeSet<>(ring.keySet());
        points.addAll(other.ring.keySet());

        List<RingDiff.Range> ranges = new ArrayList<>();
        long previous = points.last();
        for (long point : points) {
            // Every hash in (previous, point] has the same owner within each ring
            String before = ownerOf(point);
            String after = other.ownerOf(point);
            if (!before.equals(after)) {
                ranges.add(new RingDiff.Range(previous, point, before, after));
            }
            previous = point;
        }
        return new RingDiff(this, other, ranges);
    }

    // MD5 rather than hashCode(): the result must be identical on every JVM and evenly spread
    public static long hash(String value) {
        try {
            byte[] digest = MessageDigest.getInstance("MD5").digest(value.getBytes(StandardCharsets.UTF_8));
            return ByteBuffer.wrap(digest).getLong();
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
    }
}
