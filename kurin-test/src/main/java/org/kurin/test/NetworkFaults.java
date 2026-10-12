package org.kurin.test;

import java.time.Duration;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * The fault plan shared by all nodes of a {@link KurinTestCluster}: which directed links are cut,
 * slowed down or lossy. Links are identified by node ids ({@code host:port}).
 */
public final class NetworkFaults {
    private record Link(String from, String to) {
    }

    private final Set<Link> blocked = ConcurrentHashMap.newKeySet();
    private final Map<Link, Duration> delays = new ConcurrentHashMap<>();
    private final Map<Link, Double> dropRates = new ConcurrentHashMap<>();

    /**
     * Cuts every link between nodes of different groups, in both directions. Replaces an earlier partition.
     */
    public void partition(Collection<? extends Collection<String>> groups) {
        blocked.clear();
        for (Collection<String> group : groups) {
            for (Collection<String> other : groups) {
                if (group != other) {
                    for (String from : group) {
                        for (String to : other) {
                            blocked.add(new Link(from, to));
                        }
                    }
                }
            }
        }
    }

    public void delay(String from, String to, Duration delay) {
        if (delay == null || delay.isZero()) {
            delays.remove(new Link(from, to));
        } else {
            delays.put(new Link(from, to), delay);
        }
    }

    /**
     * @param rate probability between 0 and 1 that a request on the link is lost
     */
    public void dropRate(String from, String to, double rate) {
        if (rate < 0 || rate > 1) {
            throw new IllegalArgumentException("Drop rate must be between 0 and 1");
        }
        if (rate == 0) {
            dropRates.remove(new Link(from, to));
        } else {
            dropRates.put(new Link(from, to), rate);
        }
    }

    /**
     * Removes every partition, delay and drop rate.
     */
    public void heal() {
        blocked.clear();
        delays.clear();
        dropRates.clear();
    }

    boolean isBlocked(String from, String to) {
        return blocked.contains(new Link(from, to));
    }

    Duration delayOf(String from, String to) {
        return delays.getOrDefault(new Link(from, to), Duration.ZERO);
    }

    double dropRateOf(String from, String to) {
        return dropRates.getOrDefault(new Link(from, to), 0.0);
    }
}
