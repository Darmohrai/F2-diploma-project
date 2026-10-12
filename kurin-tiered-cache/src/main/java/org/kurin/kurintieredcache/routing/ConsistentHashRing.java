package org.kurin.kurintieredcache.routing;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Collection;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;

public class ConsistentHashRing {
    private final NavigableMap<Long, String> ring = new TreeMap<>();

    public ConsistentHashRing(Collection<String> nodes, int virtualNodes) {
        if (nodes == null || nodes.isEmpty()) {
            throw new IllegalArgumentException("Hash ring must have at least one node");
        }
        if (virtualNodes <= 0) {
            throw new IllegalArgumentException("virtualNodes must be positive");
        }
        for (String node : nodes) {
            for (int i = 0; i < virtualNodes; i++) {
                ring.put(hash(node + "#" + i), node);
            }
        }
    }

    public String nodeFor(String key) {
        Map.Entry<Long, String> entry = ring.ceilingEntry(hash(key));
        return (entry != null ? entry : ring.firstEntry()).getValue();
    }

    // MD5 rather than hashCode(): the result must be identical on every JVM and evenly spread
    private static long hash(String value) {
        try {
            byte[] digest = MessageDigest.getInstance("MD5").digest(value.getBytes(StandardCharsets.UTF_8));
            return ByteBuffer.wrap(digest).getLong();
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
    }
}
