package org.kurin.kurinspringbootstarter.observability;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.kurin.api.KurinObserver;
import org.kurin.raft.KurinNode;

import java.util.concurrent.TimeUnit;

/**
 * Publishes the measurements of the Raft core as Micrometer meters.
 * <p>
 * Gauges: {@code kurin.raft.role} (0 follower, 1 candidate, 2 leader), {@code kurin.raft.term},
 * {@code kurin.raft.commit.index}, {@code kurin.raft.applied.index}, {@code kurin.raft.log.entries}.
 * Counters: {@code kurin.raft.elections{result}}, {@code kurin.raft.leader.changes}.
 * Timers: {@code kurin.raft.commit.latency}, {@code kurin.raft.apply.duration{type}},
 * {@code kurin.raft.rpc.latency{type,peer}}, {@code kurin.raft.snapshot.duration}.
 * Summaries: {@code kurin.raft.append.batch.size}, {@code kurin.raft.snapshot.bytes}.
 */
public class KurinRaftMetrics implements KurinObserver {
    private final MeterRegistry registry;
    private final Counter leaderChanges;
    private final Timer commitLatency;
    private final Timer snapshotDuration;
    private final DistributionSummary appendBatchSize;
    private final DistributionSummary snapshotBytes;

    public KurinRaftMetrics(MeterRegistry registry) {
        this.registry = registry;
        this.leaderChanges = Counter.builder("kurin.raft.leader.changes")
                .description("Times this node learned of a new leader").register(registry);
        this.commitLatency = Timer.builder("kurin.raft.commit.latency")
                .description("Time from appending an entry on the leader to committing it").register(registry);
        this.snapshotDuration = Timer.builder("kurin.raft.snapshot.duration")
                .description("Time to take a snapshot and compact the log").register(registry);
        this.appendBatchSize = DistributionSummary.builder("kurin.raft.append.batch.size")
                .description("Entries per replication request").register(registry);
        this.snapshotBytes = DistributionSummary.builder("kurin.raft.snapshot.bytes")
                .baseUnit("bytes").description("Size of the state machine snapshot").register(registry);
    }

    /**
     * Registers the gauges that read the published status of the node.
     */
    public void bindTo(KurinNode node) {
        Gauge.builder("kurin.raft.role", node, n -> n.status().role().ordinal())
                .description("0 follower, 1 candidate, 2 leader").register(registry);
        Gauge.builder("kurin.raft.term", node, n -> n.status().term()).register(registry);
        Gauge.builder("kurin.raft.commit.index", node, n -> n.status().commitIndex()).register(registry);
        Gauge.builder("kurin.raft.applied.index", node, n -> n.status().lastApplied()).register(registry);
        Gauge.builder("kurin.raft.log.entries", node, KurinNode::logEntries)
                .description("Entries held in the log, not yet replaced by a snapshot").register(registry);
    }

    @Override
    public void onElection(String result) {
        registry.counter("kurin.raft.elections", "result", result).increment();
    }

    @Override
    public void onLeaderChange(String leaderId) {
        leaderChanges.increment();
    }

    @Override
    public void onCommit(long latencyNanos) {
        commitLatency.record(latencyNanos, TimeUnit.NANOSECONDS);
    }

    @Override
    public void onApply(String type, long durationNanos) {
        registry.timer("kurin.raft.apply.duration", "type", type).record(durationNanos, TimeUnit.NANOSECONDS);
    }

    @Override
    public void onRpc(String type, String peer, long latencyNanos, boolean success) {
        registry.timer("kurin.raft.rpc.latency", "type", type, "peer", peer).record(latencyNanos, TimeUnit.NANOSECONDS);
    }

    @Override
    public void onAppendBatch(int entries) {
        appendBatchSize.record(entries);
    }

    @Override
    public void onSnapshot(long durationNanos, long bytes) {
        snapshotDuration.record(durationNanos, TimeUnit.NANOSECONDS);
        snapshotBytes.record(bytes);
    }
}
