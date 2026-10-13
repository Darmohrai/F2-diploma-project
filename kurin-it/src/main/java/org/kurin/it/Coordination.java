package org.kurin.it;

import org.kurin.kurinlocks.job.KurinJobContext;
import org.kurin.kurinlocksspringbootstarter.annotation.KurinDistributedLock;
import org.kurin.kurinlocksspringbootstarter.annotation.KurinDistributedLock.OnBusy;
import org.kurin.kurinlocksspringbootstarter.annotation.KurinScheduledOnce;
import org.kurin.raft.KurinNode;
import org.springframework.stereotype.Component;

/**
 * Work that must not run on two instances at once.
 */
@Component
public class Coordination {
    private final KurinNode node;

    public Coordination(KurinNode node) {
        this.node = node;
    }

    /**
     * Triggered on every instance for the same slot; the cluster lets one of them run it.
     */
    @KurinScheduledOnce(name = "it-nightly", slot = "P1D")
    public void nightly() {
        Shared.JOB_RUNS.add(node.nodeId() + "#" + KurinJobContext.current().fencingToken());
    }

    @KurinDistributedLock(key = "'it-ledger'", ttl = "PT30S", waitTimeout = "PT20S", onBusy = OnBusy.WAIT)
    public void exclusive() {
        int inside = Shared.INSIDE_LOCK.incrementAndGet();
        Shared.MOST_INSIDE_LOCK.accumulateAndGet(inside, Math::max);
        Thread.onSpinWait();
        Shared.LOCKED_RUNS.incrementAndGet();
        Shared.INSIDE_LOCK.decrementAndGet();
    }

    // One lock, three answers to finding it busy

    @KurinDistributedLock(key = "'it-policy'", onBusy = OnBusy.SKIP)
    public String skipIfBusy() {
        Shared.POLICY_RUNS.incrementAndGet();
        return "ran";
    }

    @KurinDistributedLock(key = "'it-policy'", onBusy = OnBusy.THROW)
    public String throwIfBusy() {
        Shared.POLICY_RUNS.incrementAndGet();
        return "ran";
    }

    @KurinDistributedLock(key = "'it-policy'", waitTimeout = "300ms", onBusy = OnBusy.WAIT)
    public String waitIfBusy() {
        Shared.POLICY_RUNS.incrementAndGet();
        return "ran";
    }
}
