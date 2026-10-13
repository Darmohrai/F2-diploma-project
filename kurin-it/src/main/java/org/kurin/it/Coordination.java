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
}
