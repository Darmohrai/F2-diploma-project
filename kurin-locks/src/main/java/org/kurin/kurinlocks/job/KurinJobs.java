package org.kurin.kurinlocks.job;

import org.kurin.api.ReadConsistency;
import org.kurin.kurinlocks.job.JobCommands.ClaimJob;
import org.kurin.kurinlocks.job.JobCommands.ClaimResult;
import org.kurin.kurinlocks.job.JobCommands.CompleteJob;
import org.kurin.kurinlocks.job.JobCommands.JobHistory;
import org.kurin.kurinlocks.job.JobCommands.JobMode;
import org.kurin.kurinlocks.job.JobCommands.JobRun;
import org.kurin.raft.KurinNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Runs a job on exactly one node per time slot, when every node of the cluster triggers it.
 * <p>
 * Each node computes the slot of "now" and asks the cluster to grant it. With {@link JobMode#AT_MOST_ONCE} a
 * slot is granted once, ever: if the winner crashes mid-run, the slot is not repeated. With
 * {@link JobMode#AT_LEAST_ONCE} a slot whose owner did not report completion within the lease can be claimed
 * again, so the job may run twice for it; the second run gets a larger fencing token.
 * <p>
 * Slots are derived from the wall clock of each node, rounded to the nearest slot boundary. Nodes agree on the
 * slot as long as their clocks differ by less than half a slot from the trigger time; keep clock skew well
 * below that.
 */
public class KurinJobs {
    private static final Logger log = LoggerFactory.getLogger(KurinJobs.class);

    /**
     * Observes job claims and runs of this node, for metrics.
     */
    public interface JobListener {
        void onClaim(String jobName, boolean granted);

        void onRun(String jobName, boolean success, Duration duration);

        /**
         * The cluster gave no definite answer to a claim: the job did not run here, and it is not known whether
         * another node owns the slot. Reported instead of {@link #onClaim}.
         */
        default void onClaimUnknown(String jobName, Throwable cause) {
        }
    }

    private final KurinNode node;
    private final Duration commandTimeout;
    private final int historySize;
    private final List<JobListener> listeners = new CopyOnWriteArrayList<>();

    public KurinJobs(KurinNode node, Duration commandTimeout, int historySize) {
        this.node = node;
        this.commandTimeout = commandTimeout;
        this.historySize = historySize;
    }

    private final java.util.Set<String> knownJobs = java.util.concurrent.ConcurrentHashMap.newKeySet();

    /**
     * @return names of the jobs this instance has tried to run since it started
     */
    public java.util.Set<String> knownJobs() {
        return java.util.Set.copyOf(knownJobs);
    }

    public void addListener(JobListener listener) {
        listeners.add(listener);
    }

    /**
     * @return the number of the slot the given instant belongs to, rounded to the nearest slot boundary
     */
    public static long slotOf(long epochMillis, Duration slotDuration) {
        long slotMs = slotDuration.toMillis();
        if (slotMs <= 0) {
            throw new IllegalArgumentException("Slot duration must be at least one millisecond");
        }
        return Math.floorDiv(epochMillis + slotMs / 2, slotMs);
    }

    /**
     * Runs the job if the cluster grants the current slot to this node.
     *
     * @return the result of the job, or empty if the job did not run here: another node owns the slot, or the
     * cluster gave no definite answer to the claim. The second case is logged as a warning and reported to
     * {@link JobListener#onClaimUnknown}; a trigger that fires on a schedule has nobody to throw to.
     * @throws Exception whatever the job throws; the failure is recorded in the job history first
     */
    public <T> Optional<T> runOnce(String jobName, Duration slotDuration, JobMode mode, Duration lease, Callable<T> job) throws Exception {
        long slot = slotOf(System.currentTimeMillis(), slotDuration);
        String ownerId = node.nodeId() + ":" + UUID.randomUUID();

        knownJobs.add(jobName);
        Optional<ClaimResult> claim = claim(jobName, slot, ownerId, mode, lease);
        if (claim.isEmpty()) {
            return Optional.empty();
        }
        boolean granted = claim.get().granted();
        listeners.forEach(listener -> listener.onClaim(jobName, granted));
        if (!granted) {
            return Optional.empty();
        }

        long startedNanos = System.nanoTime();
        KurinJobContext previous = KurinJobContext.bind(new KurinJobContext(jobName, slot, claim.get().fencingToken()));
        try {
            T result = job.call();
            complete(jobName, slot, ownerId, true, null);
            notifyRun(jobName, true, startedNanos);
            return Optional.ofNullable(result);
        } catch (Exception | Error e) {
            complete(jobName, slot, ownerId, false, e.toString());
            notifyRun(jobName, false, startedNanos);
            throw e;
        } finally {
            KurinJobContext.restore(previous);
        }
    }

    private void notifyRun(String jobName, boolean success, long startedNanos) {
        Duration duration = Duration.ofNanos(System.nanoTime() - startedNanos);
        listeners.forEach(listener -> listener.onRun(jobName, success, duration));
    }

    // Empty when nothing definite is known about the claim
    private Optional<ClaimResult> claim(String jobName, long slot, String ownerId, JobMode mode, Duration lease) {
        try {
            return Optional.of(node.<ClaimResult>submitCommand(
                            new ClaimJob(jobName, slot, ownerId, lease.toMillis(), mode, historySize), commandTimeout)
                    .get(commandTimeout.toMillis() + 1000, TimeUnit.MILLISECONDS));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return claimUnknown(jobName, slot, e);
        } catch (ExecutionException | TimeoutException e) {
            return claimUnknown(jobName, slot, e instanceof ExecutionException ? e.getCause() : e);
        }
    }

    // Without a definite grant the job does not run here. If the claim was committed after all, the slot
    // simply stays unexecuted in AT_MOST_ONCE mode or is taken over after the lease in AT_LEAST_ONCE mode.
    private Optional<ClaimResult> claimUnknown(String jobName, long slot, Throwable cause) {
        log.warn("Slot {} of job '{}' is skipped on {}: the cluster gave no definite answer to the claim ({})",
                slot, jobName, node.nodeId(), cause.toString());
        listeners.forEach(listener -> listener.onClaimUnknown(jobName, cause));
        return Optional.empty();
    }

    private void complete(String jobName, long slot, String ownerId, boolean success, String error) {
        try {
            node.<Boolean>submitCommand(new CompleteJob(jobName, slot, ownerId, success, error), commandTimeout)
                    .get(commandTimeout.toMillis() + 1000, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (ExecutionException | TimeoutException e) {
            log.warn("Could not record the completion of slot {} of job '{}': {}", slot, jobName, e.toString());
        }
    }

    /**
     * @return the remembered runs of the job, oldest slot first, read with linearizable consistency
     */
    public List<JobRun> history(String jobName) {
        try {
            return node.<List<JobRun>>submitQuery(new JobHistory(jobName), ReadConsistency.LINEARIZABLE)
                    .get(commandTimeout.toMillis() + 1000, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while reading the history of job '" + jobName + "'", e);
        } catch (ExecutionException e) {
            throw e.getCause() instanceof RuntimeException runtime ? runtime : new IllegalStateException(e.getCause());
        } catch (TimeoutException e) {
            throw new org.kurin.api.exception.KurinTimeoutException("No answer for the history of job '" + jobName + "'");
        }
    }
}
